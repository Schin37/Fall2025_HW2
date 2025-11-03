package ragPipeline.IncrementalDatabaseCreation

import java.io.ByteArrayInputStream
import java.time.Instant

import org.apache.pdfbox.io.MemoryUsageSetting
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException
import org.apache.pdfbox.text.PDFTextStripper

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.broadcast
import org.slf4j.LoggerFactory

import ragPipeline.helper.{Chunker, Vectors}
import ragPipeline.models.Ollama

/**
 * Incremental builder that turns a directory of PDFs into the normalized
 * retrieval snapshot consumed by the query path.  Each invocation performs a
 * diff against a manifest of previously processed files, re-chunks only the
 * documents whose bytes changed, embeds the new text slices, and finally
 * rewrites the portion of the retrieval index that references those docs.
 *
 * The implementation is intentionally split into well named helpers so the
 * happy-path of [[run]] reads like a high level flow:
 *   1. discover changed PDFs by comparing size/mtime and SHA-256 hashes
 *   2. extract text + chunk deterministically (stable chunkId generation)
 *   3. persist new doc/chunk versions and generate missing embeddings
 *   4. materialize only the touched partitions of the retrieval index
 *   5. update lightweight run metrics for observability
 */
object IncrementalDatabasePipeline extends Serializable {

  @transient private lazy val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  // ---------- Row types ----------
  final case class DocRow(
                           docId: String,
                           uri: String,
                           title: String,
                           language: String,
                           contentHash: String,
                           text: String,
                           ingestedAt: Long
                         )

  final case class ChunkRow(
                             docId: String,
                             contentHash: String,
                             uri: String,
                             title: String,
                             language: String,
                             chunkIx: Int,
                             start: Int,
                             end: Int,
                             chunkText: String,
                             sectionPath: String,
                             chunkId: String,
                             ingestedAt: Long
                           )

  final case class EmbRow(
                           embedder: String,
                           embVersion: String,
                           docId: String,
                           //                           contentHash: String,
                           chunkId: String,
                           chunkIx: Int,
                           embedding: Array[Float],
                           embDim: Int,
                           ingestedAt: Long
                         )
  // ---------- Metrics tracking (local or AWS) ----------
  final case class RunMetrics(
                               runAtIsoUtc: String,       // ISO UTC time for readability
                               numDocsScanned: Long,
                               numChangedDocs: Long,
                               numChunksWritten: Long,
                               numEmbeddingsWritten: Long,
                               indexRows: Long,
                               durationMs: Long
                             )

  private def writeTwoRowMetricsWindow(
                                        spark: org.apache.spark.sql.SparkSession,
                                        metricsPath: String,
                                        m: RunMetrics
                                      ): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.SaveMode
    import org.apache.hadoop.fs.Path

    // Current run row
    val currentDf = Seq(m).toDF()

    // Pull last previous row into driver to avoid reading from the same path we will overwrite
    val lastPrevLocal: Seq[RunMetrics] =
      try {
        if (!pathExists(spark, metricsPath)) Seq.empty
        else {
          spark.read.parquet(metricsPath)
            .as[RunMetrics]
            .orderBy($"runAtIsoUtc".desc)
            .limit(1)
            .collect()
            .toSeq
        }
      } catch { case _: Throwable => Seq.empty }

    val windowDf =
      if (lastPrevLocal.nonEmpty) spark.createDataset(lastPrevLocal).toDF().unionByName(currentDf)
      else currentDf

    // Write to a temp dir first, then atomically replace target
    val hconf  = spark.sparkContext.hadoopConfiguration
    val target = new Path(metricsPath)
    val fs     = target.getFileSystem(hconf)

    Option(target.getParent).foreach(p => if (!fs.exists(p)) fs.mkdirs(p))

    val tmp = new Path(target.getParent, s".metrics_parquet_tmp_${System.currentTimeMillis()}")

    windowDf.coalesce(1).write.mode(SaveMode.Overwrite).parquet(tmp.toString)

    if (fs.exists(target)) fs.delete(target, true)  // remove old dir (which we might have read)
    fs.rename(tmp, target)                           // replace with fresh write
  }

  private def writeCsvFromMetricsParquet(
                                          spark: SparkSession,
                                          metricsPath: String,   // e.g. s"$outDir/_metrics"
                                          targetCsv:   String    // e.g. s"$outDir/run_metrics.csv"
                                        ): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    import spark.implicits._

    if (!pathExists(spark, metricsPath)) return

    val df = spark.read.parquet(metricsPath)
      .orderBy(col("runAtIsoUtc").desc)   // newest first
      .limit(2)                           // keep previous + current

    // write to temp dir then rename the single part file to the targetCsv path
    val conf   = spark.sparkContext.hadoopConfiguration
    val target = new Path(targetCsv)
    val fs     = target.getFileSystem(conf)

    // Ensure parent dir
    Option(target.getParent).foreach(p => if (!fs.exists(p)) fs.mkdirs(p))

    // Remove existing file if present
    if (fs.exists(target)) fs.delete(target, false)

    val tmp = new Path(target.getParent, s".metrics_tmp_${System.currentTimeMillis()}")
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(tmp.toString)

    val part = fs.listStatus(tmp).map(_.getPath)
      .find(p => p.getName.startsWith("part-") && p.getName.endsWith(".csv"))
      .getOrElse(throw new RuntimeException(s"No CSV part file found in $tmp"))

    fs.rename(part, target)
    fs.delete(tmp, true)
  }

  /** Write a single CSV file (not a folder) by writing to a temp dir then renaming the part file. */
  /** Write a single CSV file (not a folder) by writing to a temp dir then renaming the part file. */
  private def saveSingleCsv(
                             df: org.apache.spark.sql.DataFrame,
                             targetFile: String,
                             overwrite: Boolean
                           ): Unit = {
    val spark = df.sparkSession
    val conf  = spark.sparkContext.hadoopConfiguration

    val targetPath = new org.apache.hadoop.fs.Path(targetFile)
    val fs         = targetPath.getFileSystem(conf)

    // Ensure parent directory exists
    Option(targetPath.getParent).foreach { parent =>
      if (!fs.exists(parent)) fs.mkdirs(parent)
    }

    // If overwriting, remove any existing file or directory at target
    if (overwrite && fs.exists(targetPath)) fs.delete(targetPath, true)

    // Write to a temp directory as CSV (Spark always writes a folder)
    val tmpPath = new org.apache.hadoop.fs.Path(
      targetPath.getParent,
      s".metrics_tmp_${System.currentTimeMillis()}"
    )

    df.coalesce(1)
      .write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .option("header", "true")
      .csv(tmpPath.toString)

    // Find the single part file that Spark wrote
    val part = fs.listStatus(tmpPath)
      .map(_.getPath)
      .find(p => p.getName.startsWith("part-") && p.getName.endsWith(".csv"))
      .getOrElse(throw new RuntimeException(s"No CSV part file found in $tmpPath"))

    // Move/rename the part to the exact target file
    fs.rename(part, targetPath)

    // Clean temp directory
    fs.delete(tmpPath, true)
  }
  // ---------- Paths ----------
  private case class OutPaths(base: String) {
    val docsPath  = s"$base/doc_normalized"   // versioned (append)
    val chunksPath= s"$base/chunks"           // versioned (append)
    val embedsPath= s"$base/embeddings"       // versioned (append)
    val indexPath = s"$base/retrieval_index"  // snapshot (overwrite)
    // Track last-seen (uri, length, mtime) so we can skip re-reading bytes
    val manifestPath = s"$base/_manifest"
  }



  // ---------- Public entry point ----------
  /**
   * @param embedder     logical embedder name (e.g. "ollama:nomic-embed-text")
   * @param embVersion   your internal version string for the embedder + preprocessing (e.g. "v1")
   * @param shardBuckets number of shard buckets for partitioning (doc-affinity)
   */
  def run(
           spark: SparkSession,
           pdfRoot: String,
           outDir: String,
           embedder: String   = "ollama:nomic-embed-text",
           embVersion: String = "v1",
           shardBuckets: Int  = 64
         ): Unit = {
    import spark.implicits._
    import java.nio.file.{Files, Paths => JPaths}
    import java.security.MessageDigest

    val t0 = nowMs()

    val paths = OutPaths(outDir)
    logger.info(s"[Incremental] START  pdfRoot=$pdfRoot  outDir=$outDir  embedder=$embedder/$embVersion")

    // Better small files layout + dyn overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    // 1) Detect new or changed PDFs (by mtime/size)
    // 1) Detect new or changed PDFs (fast prefilter by size/mtime, then hash only candidates)
    val conf = spark.sparkContext.hadoopConfiguration

    // List all PDFs with (uri, length, mtime)
    val statsNow = hdfsListFiles(spark, pdfRoot)
    import spark.implicits._
    val statsNowDF = statsNow.toDF // columns: uri,length,mtime

    // Load previous manifest (may be empty or missing sha256 on first run)
    val prevStatsDF =
      if (pathExists(spark, paths.manifestPath)) {
        val df = spark.read.parquet(paths.manifestPath)
        if (df.columns.contains("sha256")) df else df.withColumn("sha256", lit(""))
      } else {
        // empty with expected schema
        statsNowDF.withColumn("sha256", lit("")).limit(0)
      }

    // Prefilter: new or size/mtime changed
    val candidatesURIs: Vector[String] =
      statsNowDF.as("cur")
        .join(prevStatsDF.select("uri","length","mtime").as("prev"), Seq("uri"), "left")
        .filter( col("prev.uri").isNull ||
          (col("cur.length") =!= col("prev.length") || col("cur.mtime") =!= col("prev.mtime")) )
        .select(col("uri")).as[String].collect().toVector

    // Helper: streaming SHA-256 via Hadoop FS (safe for spaces, S3, HDFS)
    def sha256ForUri(u: String): String = {
      val md = java.security.MessageDigest.getInstance("SHA-256")
      val p  = new org.apache.hadoop.fs.Path(u)
      val fs = p.getFileSystem(conf)
      val in = fs.open(p)
      val buf = new Array[Byte](8 * 1024)  // <-- FIX: give the buffer a size
      try {
        var n = in.read(buf)
        while (n > 0) { md.update(buf, 0, n); n = in.read(buf) }
      } finally in.close()
      md.digest().map("%02x".format(_)).mkString
    }

    // Compute hashes ONLY for candidates
    val candWithHash = candidatesURIs.map { u =>
      val s = statsNow.find(_.uri == u).get // safe: u came from statsNow
      (u, s.length, s.mtime, sha256ForUri(u))
    }.toDF("uri","length","mtime","sha256")

    // Join to prev with sha to decide true “changed”
    val changedUris: Vector[String] =
      candWithHash.as("cur")
        .join(prevStatsDF.select("uri","sha256").as("prev"), Seq("uri"), "left")
        .filter( col("prev.uri").isNull || col("cur.sha256") =!= col("prev.sha256") )
        .select("uri").as[String].collect().toVector

    val fileCount = statsNow.length.toLong
    logger.info(s"[Incremental] Found $fileCount PDFs; candidates=${candidatesURIs.size}; changed=${changedUris.size}")

    // Read bytes ONLY for changed files (skip if none)
    // At this point we have an explicit list of changed URIs.  Only those are
    // re-read via Spark's binaryFile source which keeps the IO footprint low
    // even when the PDF tree is large.
    val raw: Dataset[(String, Array[Byte])] =
      if (changedUris.nonEmpty) {
        logger.info(s"[Incremental] Reading ${changedUris.size} new/changed PDFs...")
        spark.read.format("binaryFile")
          .load(changedUris: _*)
          .select(col("path").as("uri"), col("content").as("bytes"))
          .as[(String, Array[Byte])]
      } else {
        logger.info("[Incremental] No content changes detected, skipping re-read.")
        spark.emptyDataset[(String, Array[Byte])]
      }








    //    val fileCount = raw.count()
    logger.info(s"[Incremental] Found $fileCount PDFs under $pdfRoot")

    val acc = spark.sparkContext.longAccumulator("pdfsProcessed")

    // 2) Extract text on executors
    // Text extraction happens per-partition so that expensive PDFBox state is
    // reused for multiple files within a partition.  Each PDF becomes a DocRow
    // that captures normalized text plus derived metadata.
    val docsDS: Dataset[DocRow] = raw.mapPartitions { it =>
      val stripper = new PDFTextStripper()
      stripper.setSortByPosition(true)

      var local = 0L
      it.flatMap { case (uri, bytes) =>
        local += 1
        if (local % 50 == 0) { acc.add(50); System.out.println(s"[inc] ~processed ${acc.value} PDFs...") }

        var in: ByteArrayInputStream = null
        var doc: PDDocument           = null
        try {
          in  = new ByteArrayInputStream(bytes)
          doc = PDDocument.load(in, /*password*/ null, MemoryUsageSetting.setupTempFileOnly())

          val extracted  = stripper.getText(doc)
          val normalized = Chunker.normalize(Option(extracted).getOrElse(""))

          if (normalized.isEmpty) {
            System.err.println(s"[Incremental] Empty text after extraction, skipping: $uri")
            None
          } else {
            val title =
              normalized.linesIterator.take(1).mkString match {
                case t if t.nonEmpty => t
                case _               => uri.split("[/\\\\]").lastOption.getOrElse(uri)
              }
            val language    = "en"
            val docId       = sha256Hex(uri)
            val contentHash = sha256Hex(normalized)
            val ts          = Instant.now().toEpochMilli
            Some(DocRow(docId, uri, title, language, contentHash, normalized, ts))
          }
        } catch {
          case _: InvalidPasswordException =>
            System.err.println(s"[Incremental] Encrypted PDF (skipping): $uri"); None
          case e: Throwable =>
            System.err.println(s"[Incremental] PDF extract failed for $uri: ${e.getClass.getSimpleName}: ${e.getMessage}")
            None
        } finally {
          if (doc != null) doc.close()
          if (in != null)  in.close()
        }
      }
    }

    // 3) Delta detection (docId, contentHash)
    val existingKeys: DataFrame =
      if (pathExists(spark, paths.docsPath))
        spark.read.parquet(paths.docsPath).select("docId", "contentHash").distinct()
      else emptyDocsDeltaFrame(spark)

    val docsDF = docsDS.toDF()

    val toProcessDF =
      if (existingKeys.isEmpty) docsDF
      else docsDF.join(broadcast(existingKeys), Seq("docId", "contentHash"), "left_anti")

    val hasWork = toProcessDF.take(1).nonEmpty


    val changedDocIds: Array[String] =
      if (hasWork) toProcessDF.select("docId").distinct().as[String].collect()
      else Array.empty[String]



    // this makes sure that in the event that this exists
    if(hasWork) {
      // 4) Deterministic chunking
      // Deterministic chunking ensures a stable chunkId for identical text so
      // we avoid re-embedding on future runs when only chunk ordering changes.
      val chunkedDF: DataFrame = toProcessDF.as[DocRow].mapPartitions { it =>
        it.flatMap { doc =>
          val pieces = Chunker.split(doc.text)
          var cursor = 0
          pieces.zipWithIndex.flatMap { case (piece, ix) =>
            val start = doc.text.indexOf(piece, cursor)
            val end   = if (start >= 0) start + piece.length else -1
            if (start >= 0) {
              cursor = end
              val cid = sha256Hex(s"${doc.docId}:${start}:${end}:${doc.contentHash}")
              Some(ChunkRow(
                doc.docId, doc.contentHash, doc.uri, doc.title, doc.language,
                ix, start, end, piece, s"/p=$ix", cid, doc.ingestedAt
              ))
            } else None
          }
        }
      }.toDF()

      // 4.1) Write versioned docs/chunks (append; keep history)
      // Use LONG shard to match Spark BIGINT
      val docsOut =
        toProcessDF
          .drop("text")
          .withColumn("shard", pmod(xxhash64(col("docId")), lit(shardBuckets.toLong)))

      val chunksOut =
        chunkedDF
          .withColumn("shard", pmod(xxhash64(col("docId")), lit(shardBuckets.toLong)))

      // Append versions (keep history)
      docsOut
        .repartition(col("shard"), col("docId"))
        .write
        .mode(SaveMode.Append)
        .option("maxRecordsPerFile", 5000)
        .partitionBy("shard", "docId")
        .parquet(paths.docsPath)

      chunksOut
        .repartition(col("shard"), col("docId"))
        .write
        .mode(SaveMode.Append)
        .option("maxRecordsPerFile", 5000)
        .partitionBy("shard", "docId")
        .parquet(paths.chunksPath)

      logger.info("[Incremental] Wrote new doc/chunk versions.")

      // =========================
      // 5) SELECT MISSING VECTORS
      // =========================
      // Join against existing embeddings to figure out which chunkIds need new
      // vectors for the requested embedder/version pair.
      val existingEmbKeys: DataFrame =
        if (pathExists(spark, paths.embedsPath))
          spark.read.parquet(paths.embedsPath)
            .filter(col("embedder") === embedder && col("embVersion") === embVersion)
            .filter(col("docId").isin(changedDocIds: _*))
            .select("docId", "chunkId").distinct()
        else emptyEmbKeysFrame(spark)

      val candChunks = chunksOut.select("docId", "contentHash", "chunkId", "chunkIx", "shard")

      val chunksNeedingVecs =
        if (existingEmbKeys.isEmpty) candChunks
        else candChunks.join(broadcast(existingEmbKeys), Seq("docId", "chunkId"), "left_anti")

      if (chunksNeedingVecs.take(1).nonEmpty) {
        val toEmbedDS =
          chunksNeedingVecs
            .join(
              chunksOut.select($"docId", $"chunkId", $"chunkText"),
              Seq("docId", "chunkId")
            )
            // NOTE: shard typed as Long here
            .select($"docId", $"chunkId", $"chunkText", $"chunkIx", $"shard")
            .as[(String, String, String, Int, Long)]

        // Embedding work is grouped into small batches per partition.  Doing
        // it here keeps Ollama calls close to the data and allows us to reuse
        // HTTP clients instead of thrashing them for every chunk.
        val embDS: Dataset[EmbRow] = toEmbedDS.mapPartitions { rows =>
          val client    = new Ollama()
          val batchSize = 64
          rows.grouped(batchSize).flatMap { group =>
            val texts   = group.map(_._3).toVector
            val vectors = if (texts.nonEmpty) client.embed(texts).map(Vectors.l2Normalization) else Vector.empty
            val now     = Instant.now().toEpochMilli
            val n       = math.min(group.size, vectors.size)
            (0 until n).iterator.map { i =>
              val (docId, chunkId, _txt, chunkIx, _shard) = group(i)
              val vec   = vectors(i)
              EmbRow(
                embedder     = embedder,
                embVersion   = embVersion,
                docId        = docId,
                //                contentHash  = "", // filled on join below
                chunkId      = chunkId,
                chunkIx      = chunkIx,
                embedding    = vec,
                embDim       = vec.length,
                ingestedAt   = now
              )
            }
          }
        }

        val embDF = embDS
          .toDF()
          .join(chunksOut.select("docId", "chunkId", "contentHash", "shard"), Seq("docId", "chunkId"))
          .select(
            col("embedder"), col("embVersion"),
            col("docId"),    col("contentHash"),
            col("chunkId"),  col("chunkIx"),
            col("embedding"),col("embDim"),
            col("ingestedAt"),
            col("shard")
          )

        embDF
          .repartition(col("embedder"), col("embVersion"), col("shard"), col("docId"))
          .write.mode(SaveMode.Append)
          .option("maxRecordsPerFile", 5000)
          .partitionBy("embedder", "embVersion", "shard", "docId")
          .parquet(paths.embedsPath)

        logger.info(s"[Incremental] Wrote ${embDF.count()} new embeddings for $embedder/$embVersion.")
      } else {
        logger.info(s"[Incremental] No missing vectors for $embedder/$embVersion.")
      }
    }
    else{
      logger.info("[Incremental] No content changes detected. Nothing to do.")
    }



    // =========================
    // 7) MATERIALIZE SNAPSHOT INDEX
    // =========================
    if (changedDocIds.nonEmpty) {
      buildAndPublishIndex(spark, paths, embedder, embVersion, changedDocIds)
    }

    // Update manifest so next run can skip unchanged files cheaply
    val manifestDF =
      statsNowDF
        .join(prevStatsDF.select($"uri".as("p_uri"), $"sha256".as("prevSha")),
          statsNowDF("uri") === col("p_uri"), "left")
        .drop("p_uri")
        .join(
          candWithHash.select($"uri".as("c_uri"), $"sha256".as("newSha")),
          statsNowDF("uri") === col("c_uri"), "left"
        )
        .withColumn("sha256", coalesce(col("newSha"), col("prevSha"), lit("")))
        .select("uri","length","mtime","sha256")

    manifestDF.write.mode(SaveMode.Overwrite).parquet(paths.manifestPath)

    // =========================
    // 8) VERIFY DELTA BEHAVIOR + METRICS
    // =========================

    val numChunksWritten: Long =
      if (hasWork && pathExists(spark, paths.chunksPath))
        spark.read.parquet(paths.chunksPath).filter(col("ingestedAt") >= t0).count()
      else 0L

    val numEmbeddingsWritten: Long =
      if (hasWork && pathExists(spark, paths.embedsPath))
        spark.read.parquet(paths.embedsPath)
          .filter(col("ingestedAt") >= t0)
          .count()
      else 0L

    val numIndexRows: Long =
      if (pathExists(spark, paths.indexPath))
        spark.read.parquet(paths.indexPath).count()
      else 0L

    val metrics = RunMetrics(
      runAtIsoUtc          = java.time.Instant.now().toString,
      numDocsScanned       = fileCount,
      numChangedDocs       = if (hasWork) toProcessDF.count() else 0L,
      numChunksWritten     = numChunksWritten,
      numEmbeddingsWritten = numEmbeddingsWritten,
      indexRows            = numIndexRows,
      durationMs           = nowMs() - t0
    )

    // -- write metrics parquet window first
    writeTwoRowMetricsWindow(spark, s"$outDir/_metrics_parquet", metrics)

    spark.catalog.refreshByPath(s"$outDir/_metrics_parquet")
    val window2 = spark.read.parquet(s"$outDir/_metrics_parquet")
      .orderBy(col("runAtIsoUtc").desc)
      .limit(2)
      .cache()

    // force materialization so writer won't race with lingering deletes
    window2.count()

    // -- write a single CSV file atomically (temp dir -> rename)
    saveSingleCsv(window2, s"$outDir/run_metrics.csv", overwrite = true)
    window2.unpersist()

    logger.info(s"[pipeline] Metrics updated: $outDir/_metrics (parquet) and $outDir/run_metrics.csv")
  }

  // ---------- Snapshot index (Step 7) ----------
  private def buildAndPublishIndex(
                                    spark: SparkSession,
                                    paths: OutPaths,
                                    embedder: String,
                                    embVersion: String,
                                    changedDocIds: Array[String]          // NEW
                                  ): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SaveMode


    if (!pathExists(spark, paths.docsPath) || !pathExists(spark, paths.chunksPath)) {
      logger.warn("[Index] Docs or Chunks do not exist yet. Skipping index build.")
      return
    }

    // Ensure dynamic partition overwrite is on
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val docs   = spark.read.parquet(paths.docsPath)
    val chunks = spark.read.parquet(paths.chunksPath)

    // Only rows for changed docIds
    val changedDocs   = docs.where(col("docId").isin(changedDocIds:_*))
    val changedChunks = chunks.where(col("docId").isin(changedDocIds:_*))

    // Latest version for the changed docIds only
    val latestChangedDocs = changedDocs
      .withColumn("rn",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy($"docId").orderBy($"ingestedAt".desc)
        )
      )
      .where($"rn" === 1)
      .drop("rn")

    val latestChangedChunks = latestChangedDocs
      .select($"docId".as("d_id"), $"contentHash".as("d_hash"), $"title", $"language", $"shard")
      .join(
        changedChunks.select($"docId".as("c_id"), $"contentHash".as("c_hash"),
          $"chunkId", $"chunkIx", $"chunkText", $"sectionPath"),
        $"d_id" === $"c_id" && $"d_hash" === $"c_hash"
      )
      .select(
        $"d_id".as("docId"),
        $"d_hash".as("contentHash"),
        $"title", $"language", $"shard",
        $"chunkId", $"chunkIx", $"chunkText", $"sectionPath"
      )

    // Embeddings are already partitioned by (embedder, embVersion, shard, docId)
    if (!pathExists(spark, paths.embedsPath)) {
      logger.warn("[Index] Embeddings path not found; nothing to write for changed docs.")
      return
    }

    val emb = spark.read.parquet(paths.embedsPath)
      .where(col("embedder") === embedder && col("embVersion") === embVersion)
      .where(col("docId").isin(changedDocIds:_*))
      .select("docId", "chunkId", "embedding", "embDim")

    // Only re-materialize the subset of the retrieval index that references
    // the docIds modified in this run.  The rest of the parquet files are left
    // untouched which keeps reruns fast.
    val changedIndexDF = latestChangedChunks
      .join(emb, Seq("docId", "chunkId"))
      .select(
        $"shard", $"docId", $"contentHash",
        $"title", $"language",
        $"chunkId", $"chunkIx", $"sectionPath", $"chunkText",
        $"embedding", $"embDim"
      )

    // Overwrite ONLY the touched partitions
    changedIndexDF
      .repartition(col("shard"), col("docId"))
      .write
      .mode(SaveMode.Overwrite)
      .option("maxRecordsPerFile", 5000)
      .partitionBy("shard","docId")                      // <<< partition by docId too
      .parquet(paths.indexPath)

    logger.info(s"[Index] Incrementally updated ${changedDocIds.length} docId(s) at ${paths.indexPath}")
  }

  // ---------- helpers ----------
  // Lightweight file stat we can diff without reading bytes
  final case class FileStat(uri: String, length: Long, mtime: Long)

  private def hdfsListFiles(spark: SparkSession, root: String): Vector[FileStat] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs   = new org.apache.hadoop.fs.Path(root).getFileSystem(conf)

    def listRec(p: org.apache.hadoop.fs.Path): Vector[FileStat] = {
      val it = fs.listStatusIterator(p)
      val out = scala.collection.mutable.ArrayBuffer.empty[FileStat]
      while (it.hasNext) {
        val st = it.next()
        if (st.isDirectory) out ++= listRec(st.getPath)
        else {
          val uri = st.getPath.toString
          if (uri.toLowerCase.endsWith(".pdf"))
            out += FileStat(uri, st.getLen, st.getModificationTime)
        }
      }
      out.toVector
    }

    listRec(new org.apache.hadoop.fs.Path(root))
  }

  private def emptyStatDF(spark: SparkSession): org.apache.spark.sql.DataFrame = {
    import org.apache.spark.sql.types._
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      StructType(Seq(
        StructField("uri",   StringType, nullable = false),
        StructField("length",LongType,   nullable = false),
        StructField("mtime", LongType,   nullable = false)
      ))
    )
  }

  private def sha256Hex(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.update(s.getBytes("UTF-8"))
    md.digest().map("%02x".format(_)).mkString
  }

  private def pathExists(spark: SparkSession, path: String): Boolean =
    try { spark.read.parquet(path).limit(1).count() >= 0 } catch { case _: Throwable => false }

  private def emptyDocsDeltaFrame(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("docId", StringType,   nullable = false),
        StructField("contentHash", StringType, nullable = false)
      ))
    )

  private def emptyEmbKeysFrame(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("docId", StringType,   nullable = false),
        StructField("chunkId", StringType, nullable = false)
      ))
    )


  private def nowMs(): Long = java.time.Instant.now().toEpochMilli

  private def writeMetrics(spark: SparkSession, metrics: RunMetrics, outDir: String): Unit = {
    import spark.implicits._
    // Append one row per run so you can graph/inspect later
    Seq(metrics).toDF
      .write
      .mode("append")
      .json(s"$outDir/_metrics")  // e.g. <root>/out/_metrics/*.json
  }

  private def fileExists(spark: SparkSession, path: String): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val p    = new org.apache.hadoop.fs.Path(path)
    val fs   = p.getFileSystem(conf)
    fs.exists(p)
  }
}
