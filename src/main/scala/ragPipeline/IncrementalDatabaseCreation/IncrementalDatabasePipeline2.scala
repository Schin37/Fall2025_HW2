package ragPipeline.IncrementalDatabaseCreation

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, LocatedFileStatus, RemoteIterator, Path => HPath}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, DataOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/**
 * IncrementalDatabasePipeline
 *
 * End-to-end incremental pipeline:
 * 1) List PDFs + fast change prefilter (uri,length,mtime)
 * 2) Read + normalize text; strong contentHash
 * 3) Gate by (docId, contentHash); chunk only changed docs (stable chunkId)
 * 4) Embed only missing (chunkId, contentHash, embedder, embedderVer)
 * 5) Idempotent rewrites; versioned publish of retrieval_index + atomic pointer swap
 *
 * Tables (Parquet, path-based):
 *   outDir/doc_normalized
 *   outDir/chunks
 *   outDir/embeddings
 *   outDir/manifest
 *   outDir/snapshots/retrieval_index/<ts>/  (immutable snapshots)
 *   outDir/retrieval_index                   (current pointer)
 *
 * Run via `run(...)`.
 */

object IncrementalDatabasePipeline2 {

  // ---------- Logger ----------
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  // ---------- Schemas / case classes ----------
  case class DocStat(uri: String, length: Long, mtime: Long)

  case class DocRow(
                     docId: String,
                     uri: String,
                     title: String,
                     language: String,
                     text: String,
                     contentHash: String
                   )

  case class ChunkRow(
                       chunkId: String,
                       docId: String,
                       contentHash: String,
                       chunkIx: Int,
                       start: Int,
                       end: Int,
                       sectionPath: String,
                       chunkText: String
                     )

  case class EmbeddingRow(
                           chunkId: String,
                           contentHash: String,
                           embedder: String,
                           embedderVer: String,
                           embedding: Seq[Float],
                           created_ts: String
                         )

  

  private case class OutPaths(base: String) {
    val docsPath       = s"$base/doc_normalized"
    val chunksPath     = s"$base/chunks"
    val embedPath      = s"$base/embeddings"
    val manifestPath   = s"$base/manifest"
    val indexPath      = s"$base/retrieval_index"
    val indexTmpPath   = s"$base/_tmp_retrieval_index"
    val snapsBase      = s"$base/snapshots/retrieval_index"
    def snapPath(ts: String): String = s"$snapsBase/$ts"
  }

  private val embeddingType = ArrayType(FloatType, containsNull = false)

  // ---------- Embedder ----------
  trait TextEmbedder extends Serializable {
    def embedBatch(texts: Seq[String]): Seq[Array[Float]]
  }

  /** Simple Ollama client (swap with your /mnt/data/Ollama.scala if you prefer) */
  final class DefaultEmbedder(model: String, connectTimeoutMs: Int = 15000, readTimeoutMs: Int = 60000)
    extends TextEmbedder {
    override def embedBatch(texts: Seq[String]): Seq[Array[Float]] = {
      texts.map { t =>
        val url  = new URL("http://127.0.0.1:11434/api/embeddings")
        val conn = url.openConnection().asInstanceOf[HttpURLConnection]
        conn.setConnectTimeout(connectTimeoutMs)
        conn.setReadTimeout(readTimeoutMs)
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setDoOutput(true)

        val payload = s"""{"model":${jsonString(model)},"input":${jsonString(t)}}"""
        val out = new DataOutputStream(conn.getOutputStream)
        out.write(payload.getBytes(StandardCharsets.UTF_8)); out.flush(); out.close()

        val code = conn.getResponseCode
        val is   = if (code >= 200 && code < 300) conn.getInputStream else conn.getErrorStream

        val reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        val sb = new StringBuilder; var line: String = null
        while ({ line = reader.readLine(); line != null }) sb.append(line)
        reader.close(); conn.disconnect()

        val raw = sb.toString
        val key = "\"embedding\":"
        val idx = raw.indexOf(key)
        require(idx >= 0, s"Ollama returned no embedding: $raw")
        val st = raw.indexOf('[', idx); val en = raw.indexOf(']', st)
        val arr = raw.substring(st + 1, en)
        if (arr.trim.isEmpty) Array.emptyFloatArray
        else arr.split(",").iterator.map(_.trim.toFloat).toArray
      }
    }

    private def jsonString(s: String): String =
      "\"" + s.flatMap {
        case '"'  => "\\\""
        case '\\' => "\\\\"
        case '\b' => "\\b"
        case '\f' => "\\f"
        case '\n' => "\\n"
        case '\r' => "\\r"
        case '\t' => "\\t"
        case c    => c.toString
      } + "\""
  }

  // ---------- Public entry ----------
  def run(
           spark: SparkSession,
           pdfRoot: String,
           outDir: String,
           embedder: String = "ollama",
           embedderVer: String = "mxbai-embed-large:latest",
           chunkChars: Int = 1000,
           chunkOverlap: Int = 120,
           batchSize: Int = 64,
           maxPerPart: Int = 64,
           model: String = "mxbai-embed-large"
         ): Unit = {
    import spark.implicits._
    val paths = OutPaths(outDir)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    // ---------- Ensure base tables exist (avoid PATH_NOT_FOUND on first run) ----------
    ensureEmptyTablesExist(spark, paths)

    // Also ensure retrieval_index pointer exists (bootstrap empty snapshot if needed)
    ensureEmptyRetrievalIndexExists(spark, paths)

    // ---------- List PDFs (Windows-safe normalization) ----------
    val statsNow   = hdfsListPdfs(spark, pdfRoot)
    val statsNowDF = spark.createDataset(statsNow).toDF // (uri,length,mtime)

    // ---------- Load previous manifest ----------
    val prevManifestDF = spark.read.parquet(paths.manifestPath)

    // ---------- Candidate prefilter by (uri,length,mtime) ----------
    val candidatesDF = statsNowDF.as("now")
      .join(prevManifestDF.select("uri","length","mtime").as("prev"), Seq("uri","length","mtime"), "left_anti")

    val hasCandidates = candidatesDF.take(1).nonEmpty
    logger.info(s"[Incremental] Prefilter candidates: ${candidatesDF.count()}  hasCandidates=$hasCandidates")

    // ---------- Extract+normalize & strong hash ----------
    val textDF =
      if (hasCandidates) {
        candidatesDF
          .map(_.getAs[String]("uri"))
          .mapPartitions { it =>
            it.flatMap { uri =>
              val maybe = PdfLoader.readAndNormalize(uri)
              maybe.map { case (title, language, text) =>
                val docId = stableDocId(uri)
                val hash  = sha256(text)
                DocRow(docId, uri, title, language, text, hash)
              }
            }
          }.toDF
      } else spark.emptyDataset[DocRow].toDF

    // ---------- Final gate by (docId, contentHash) ----------
    val prevDocsDF = spark.read.parquet(paths.docsPath)
    val toProcessDocsDF = textDF.as("new")
      .join(prevDocsDF.select("docId","contentHash").as("prev"), Seq("docId","contentHash"), "left_anti")
      .cache()

    val hasWork = toProcessDocsDF.take(1).nonEmpty
    logger.info(s"[Incremental] New/changed docs after hash gate: hasWork=$hasWork  count=${toProcessDocsDF.count()}")

    // ---------- Chunk only changed docs ----------
    val chunksDF: DataFrame =
      if (hasWork) {
        toProcessDocsDF.as[DocRow].flatMap(d => deterministicChunks(d, chunkChars, chunkOverlap)).toDF
      } else spark.emptyDataset[ChunkRow].toDF

    // ---------- Upsert docs/chunks + manifest (rewrite-atomic) ----------
    if (hasWork) {
      // DOCS: small table — keep simple. If you want, you can partition by docId.
      val updatedDocs = prevDocsDF.unionByName(toProcessDocsDF)
        .dropDuplicates("docId","contentHash")
        .withColumn("version_ts", current_timestamp().cast(StringType))

      updatedDocs
        .coalesce(1) // optional: keep small file count
        .write
        .mode(SaveMode.Overwrite)
        .parquet(paths.docsPath)

      // CHUNKS: write ONLY the changed partitions (dynamic partition overwrite)
      // Note: chunksDF already has only the changed docs’ chunks
      chunksDF
        .write
        .mode(SaveMode.Overwrite)              // with partitionOverwriteMode=dynamic, only touched partitions are replaced
        .partitionBy("docId","contentHash")
        .parquet(paths.chunksPath)

      // MANIFEST: typically small — union & rewrite is fine
      val prevSlim = prevManifestDF.select("uri","length","mtime","docId","contentHash")
      val newManifest = toProcessDocsDF.select("uri","docId","contentHash")
        .join(statsNowDF.select("uri","length","mtime"), Seq("uri"))
      val updatedManifest = prevSlim.unionByName(newManifest).dropDuplicates("uri","length","mtime")

      updatedManifest
        .coalesce(1) // optional
        .write
        .mode(SaveMode.Overwrite)
        .parquet(paths.manifestPath)
    }

    // ---------- Embeddings only for missing keys ----------
    val chunksForEmbDF = spark.read.parquet(paths.chunksPath)
    val prevEmbDF      = spark.read.parquet(paths.embedPath)

    val needEmbDF = chunksForEmbDF.as("c")
      .join(
        prevEmbDF
          .filter(col("embedder") === lit(embedder) && col("embedderVer") === lit(embedderVer))
          .select("chunkId","contentHash").as("e"),
        Seq("chunkId","contentHash"),
        "left_anti"
      )
      .select("chunkId","contentHash","chunkText")
      .repartition(col("chunkId"))

    val embedCount = needEmbDF.count()
    logger.info(s"[Incremental] Chunks needing embeddings for [$embedder/$embedderVer]: $embedCount")

    val embedderClient: TextEmbedder = new DefaultEmbedder(model)

    val newlyEmbedded: DataFrame =
      if (embedCount > 0) {
        needEmbDF.mapPartitions { it =>
          val arr = it.toArray
          arr.grouped(batchSize).flatMap { g =>
            val texts = g.map(_.getAs[String]("chunkText"))
            val out = texts.grouped(maxPerPart).flatMap { mini =>
              val vecs = embedderClient.embedBatch(mini)
              mini.zip(vecs)
            }.toArray
            out.zip(g).map { case ((_, vec), row) =>
              EmbeddingRow(
                chunkId     = row.getAs[String]("chunkId"),
                contentHash = row.getAs[String]("contentHash"),
                embedder    = embedder,
                embedderVer = embedderVer,
                embedding   = vec.toSeq,
                created_ts  = java.time.Instant.now.toString
              )
            }
          }
        }.toDF
      } else spark.emptyDataset[EmbeddingRow].toDF

    if (embedCount > 0) {
      // Write only the fresh rows; partition so reads/writes scale and pruning kicks in
      newlyEmbedded
        .write
        .mode(SaveMode.Append)
        .partitionBy("embedder","embedderVer","contentHash")
        .parquet(paths.embedPath)
    }

    // ---------- Build retrieval index for this embedder/version ----------
    val finalChunks = spark.read.parquet(paths.chunksPath)
      .select("chunkId","docId","contentHash","chunkIx","start","end","sectionPath","chunkText")
    val finalEmb = spark.read.parquet(paths.embedPath)
      .filter(col("embedder") === lit(embedder) && col("embedderVer") === lit(embedderVer))
      .select("chunkId","contentHash","embedding","created_ts")

    // Optionally skip publish if either side empty (keeps previous pointer live)
    if (finalChunks.take(1).isEmpty || finalEmb.take(1).isEmpty) {
      logger.info("[Incremental] Nothing to publish for this embedder/version; leaving pointer as-is.")
      return
    }

    val retrievalIndex = finalChunks.as("c")
      .join(finalEmb.as("e"), Seq("chunkId","contentHash"), "inner")
      .select(
        col("c.chunkId"),
        col("c.docId"),
        col("c.contentHash"),
        col("c.chunkIx"),
        col("c.start"),
        col("c.end"),
        col("c.sectionPath"),
        col("c.chunkText"),
        col("e.embedding"),
        col("e.created_ts").as("embedding_ts"),
        lit(embedder).as("embedder"),
        lit(embedderVer).as("embedderVer"),
        current_timestamp().cast(StringType).as("published_ts")
      )

    // ---------- Versioned publish + atomic pointer swap ----------
    val ts = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
      .withZone(java.time.ZoneId.of("UTC")).format(java.time.Instant.now())
    val snapDir = paths.snapPath(ts)

    // Write snapshot (immutable)
    rewriteAtomically(spark, retrievalIndex, snapDir)

    // Update CURRENT.json under snapshots
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val currentTmp = new HPath(s"${paths.snapsBase}/CURRENT.json.tmp")
    val current    = new HPath(s"${paths.snapsBase}/CURRENT.json")
    val out: FSDataOutputStream = fs.create(currentTmp, true)
    out.write(s"""{"version":"$ts","embedder":"$embedder","embedderVer":"$embedderVer"}""".getBytes("UTF-8"))
    out.close()
    if (fs.exists(current)) fs.delete(current, false)
    fs.rename(currentTmp, current)

    // Also update the public pointer by writing the index fresh (separate write to keep snapshot intact)
    rewriteAtomically(spark, retrievalIndex, paths.indexTmpPath)
    moveOverwrite(spark, paths.indexTmpPath, paths.indexPath)

    logger.info(s"[Incremental] Published retrieval_index version=$ts  embedder=$embedder  ver=$embedderVer")
  }

  // ---------- Deterministic chunking ----------
  private def deterministicChunks(d: DocRow, chunkChars: Int, overlap: Int): Seq[ChunkRow] = {
    val pieces = splitFixed(d.text, chunkChars, overlap)
    var cursor = 0
    pieces.zipWithIndex.map { case (piece, ix) =>
      val start = cursor
      val end   = cursor + piece.length
      cursor = end
      val sid = stableChunkId(d.docId, d.contentHash, start, end)
      ChunkRow(sid, d.docId, d.contentHash, ix, start, end, "", piece)
    }
  }

  private def splitFixed(s: String, size: Int, overlap: Int): Seq[String] = {
    if (s.isEmpty) return Seq.empty
    val safeOverlap = math.max(0, math.min(overlap, size / 2))
    val stride = math.max(1, size - safeOverlap)
    val out = scala.collection.mutable.ArrayBuffer[String]()
    var i = 0
    while (i < s.length) {
      val end = math.min(i + size, s.length)
      out += s.substring(i, end)
      if (end == s.length) i = end else i = i + stride
    }
    out.toSeq
  }

  // ---------- PDF loader (stub; replace with your real PDFBox/Tika) ----------
  object PdfLoader {
    def readAndNormalize(uri: String): Option[(String, String, String)] = {
      // NOTE: This stub treats input as plain text. Swap with PDFBox/Tika for real PDFs.
      try {
        val src  = scala.io.Source.fromFile(uri.replace('\\','/'), "UTF-8")
        val raw  = try src.getLines().mkString("\n") finally src.close()
        val title = new java.io.File(uri).getName
        val lang  = "en"
        val text  = normalize(raw)
        Some((title, lang, text))
      } catch { case _: Throwable => None }
    }
    private def normalize(s: String): String = s.replaceAll("\\s+", " ").trim
  }

  // ---------- Filesystem helpers ----------
  private def hdfsListPdfs(spark: SparkSession, root: String): Seq[DocStat] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val normalized = root.replace('\\', '/')
    val rootPath = new HPath(normalized)
    val fs = rootPath.getFileSystem(conf)

    if (!fs.exists(rootPath)) return Seq.empty

    val buf = scala.collection.mutable.ArrayBuffer[DocStat]()
    val it: RemoteIterator[LocatedFileStatus] = fs.listFiles(rootPath, true)
    while (it.hasNext) {
      val st   = it.next()
      val p    = st.getPath
      val name = p.getName.toLowerCase
      if (name.endsWith(".pdf")) {
        buf += DocStat(p.toString, st.getLen, st.getModificationTime)
      }
    }
    buf.toSeq
  }

  private def pathExists(spark: SparkSession, path: String): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.exists(new HPath(path))
  }

  /** Atomic-ish rewrite: write to tmp then rename over target. */
  private def rewriteAtomically(spark: SparkSession, df: DataFrame, targetPath: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tmpPath = new HPath(targetPath + "_tmp_write_" + System.currentTimeMillis())
    val tgtPath = new HPath(targetPath)
    df.write.mode(SaveMode.Overwrite).parquet(tmpPath.toString)
    if (fs.exists(tgtPath)) fs.delete(tgtPath, true)
    fs.rename(tmpPath, tgtPath)
  }

  /** Rename src → dest, overwriting if exists. */
  private def moveOverwrite(spark: SparkSession, src: String, dest: String): Unit = {
    val fs   = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pSrc = new HPath(src)
    val pDst = new HPath(dest)
    if (fs.exists(pDst)) fs.delete(pDst, true)
    fs.rename(pSrc, pDst)
  }

  // ---------- Table bootstrap helpers ----------
  private def ensureEmptyTablesExist(spark: SparkSession, paths: OutPaths): Unit = {
    def ensureTable(path: String, dfSchema: StructType): Unit = {
      if (!pathExists(spark, path)) {
        val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfSchema)
        rewriteAtomically(spark, empty, path)
      }
    }

    // docs
    ensureTable(paths.docsPath,
      StructType(Seq(
        StructField("docId", StringType, false),
        StructField("uri", StringType, false),
        StructField("title", StringType, false),
        StructField("language", StringType, false),
        StructField("contentHash", StringType, false),
        StructField("version_ts", StringType, true)
      ))
    )

    // chunks
    ensureTable(paths.chunksPath,
      StructType(Seq(
        StructField("chunkId", StringType, false),
        StructField("docId", StringType, false),
        StructField("contentHash", StringType, false),
        StructField("chunkIx", IntegerType, false),
        StructField("start", IntegerType, false),
        StructField("end", IntegerType, false),
        StructField("sectionPath", StringType, false),
        StructField("chunkText", StringType, false)
      ))
    )

    // embeddings (include embedder columns so downstream filters never fail)
    ensureTable(paths.embedPath,
      StructType(Seq(
        StructField("chunkId", StringType, false),
        StructField("contentHash", StringType, false),
        StructField("embedder", StringType, false),
        StructField("embedderVer", StringType, false),
        StructField("embedding", embeddingType, false),
        StructField("created_ts", StringType, false)
      ))
    )

    // manifest
    ensureTable(paths.manifestPath,
      StructType(Seq(
        StructField("uri", StringType, false),
        StructField("length", LongType,  false),
        StructField("mtime", LongType,   false),
        StructField("docId", StringType, false),
        StructField("contentHash", StringType, false)
      ))
    )
  }

  private def ensureEmptyRetrievalIndexExists(spark: SparkSession, paths: OutPaths): Unit = {
    if (!pathExists(spark, paths.indexPath)) {
      val schema = StructType(Seq(
        StructField("chunkId", StringType, false),
        StructField("docId", StringType, false),
        StructField("contentHash", StringType, false),
        StructField("chunkIx", IntegerType, false),
        StructField("start", IntegerType, false),
        StructField("end", IntegerType, false),
        StructField("sectionPath", StringType, false),
        StructField("chunkText", StringType, false),
        StructField("embedding", embeddingType, false),
        StructField("embedding_ts", StringType, false),
        StructField("embedder", StringType, false),
        StructField("embedderVer", StringType, false),
        StructField("published_ts", StringType, false)
      ))
      val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

      // write a bootstrap snapshot and point the pointer there
      rewriteAtomically(spark, empty, paths.snapPath("bootstrap"))
      rewriteAtomically(spark, empty, paths.indexTmpPath)
      moveOverwrite(spark, paths.indexTmpPath, paths.indexPath)

      // create CURRENT.json
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val current = new HPath(s"${paths.snapsBase}/CURRENT.json")
      val tmp     = new HPath(s"${paths.snapsBase}/CURRENT.json.tmp")
      val out: FSDataOutputStream = fs.create(tmp, true)
      out.write("""{"version":"bootstrap","embedder":"","embedderVer":""}""".getBytes("UTF-8"))
      out.close()
      if (fs.exists(current)) fs.delete(current, false)
      fs.rename(tmp, current)
    }
  }

  // ---------- Stable IDs & hashing ----------
  private def stableDocId(uri: String): String =
    "doc_" + sha1(uri)

  private def stableChunkId(docId: String, contentHash: String, start: Int, end: Int): String =
    "ch_" + sha1(s"$docId|$contentHash|$start|$end")

  private def sha1(s: String): String  = digestHex("SHA-1",   s.getBytes(StandardCharsets.UTF_8))
  private def sha256(s: String): String = digestHex("SHA-256", s.getBytes(StandardCharsets.UTF_8))

  private def digestHex(alg: String, bytes: Array[Byte]): String = {
    val md = MessageDigest.getInstance(alg)
    md.update(bytes)
    md.digest().map("%02x".format(_)).mkString
  }
}
