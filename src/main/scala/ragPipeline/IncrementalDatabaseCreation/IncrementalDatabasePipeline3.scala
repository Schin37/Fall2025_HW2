//package ragPipeline.IncrementalDatabaseCreation
//
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.slf4j.LoggerFactory
//
//object IncrementalDatabasePipeline3 extends Serializable {
//
//  @transient private lazy val logger = LoggerFactory.getLogger(getClass)
//
//  // ---------- Row types ----------
//  final case class DocRow(
//                           docId: String,
//                           uri: String,
//                           title: String,
//                           language: String,
//                           contentHash: String,
//                           text: String,
//                           ingestedAt: Long
//                         )
//
//  final case class ChunkRow(
//                             docId: String,
//                             contentHash: String,
//                             uri: String,
//                             title: String,
//                             language: String,
//                             chunkIx: Int,
//                             start: Int,
//                             end: Int,
//                             chunkText: String,
//                             sectionPath: String,
//                             chunkId: String,
//                             ingestedAt: Long
//                           )
//
//  final case class EmbRow(
//                           embedder: String,
//                           embVersion: String,
//                           docId: String,
//                           chunkId: String,
//                           chunkIx: Int,
//                           embedding: Array[Float],
//                           embDim: Int,
//                           ingestedAt: Long
//                         )
//
//  // ---------- Metrics tracking (local or AWS) ----------
//  final case class RunMetrics(
//                               runAtIsoUtc: String,       // ISO UTC time for readability
//                               numDocsScanned: Long,
//                               numChangedDocs: Long,
//                               numChunksWritten: Long,
//                               numEmbeddingsWritten: Long,
//                               indexRows: Long,
//                               durationMs: Long
//                             )
//
//  private def writeTwoRowMetricsWindow(
//                                        spark: org.apache.spark.sql.SparkSession,
//                                        metricsPath: String,
//                                        m: RunMetrics
//                                      ): Unit = {
//    import org.apache.hadoop.fs.Path
//    import org.apache.spark.sql.SaveMode
//    import spark.implicits._
//
//    // Current run row
//    val currentDf = Seq(m).toDF()
//
//    // Pull last previous row into driver to avoid reading from the same path we will overwrite
//    val lastPrevLocal: Seq[RunMetrics] =
//      try {
//        if (!pathExists(spark, metricsPath)) Seq.empty
//        else {
//          spark.read.parquet(metricsPath)
//            .as[RunMetrics]
//            .orderBy($"runAtIsoUtc".desc)
//            .limit(1)
//            .collect()
//            .toSeq
//        }
//      } catch { case _: Throwable => Seq.empty }
//
//    val windowDf =
//      if (lastPrevLocal.nonEmpty) spark.createDataset(lastPrevLocal).toDF().unionByName(currentDf)
//      else currentDf
//
//    // Write to a temp dir first, then atomically replace target
//    val hconf  = spark.sparkContext.hadoopConfiguration
//    val target = new Path(metricsPath)
//    val fs     = target.getFileSystem(hconf)
//
//    Option(target.getParent).foreach(p => if (!fs.exists(p)) fs.mkdirs(p))
//
//    val tmp = new Path(target.getParent, s".metrics_parquet_tmp_${System.currentTimeMillis()}")
//
//    windowDf.coalesce(1).write.mode(SaveMode.Overwrite).parquet(tmp.toString)
//
//    if (fs.exists(target)) fs.delete(target, true)  // remove old dir (which we might have read)
//    fs.rename(tmp, target)                           // replace with fresh write
//  }
//
//  /** Write a single CSV file (not a folder) by writing to a temp dir then renaming the part file. */
//  private def saveSingleCsv(
//                             df: org.apache.spark.sql.DataFrame,
//                             targetFile: String,
//                             overwrite: Boolean
//                           ): Unit = {
//    val spark = df.sparkSession
//    val conf  = spark.sparkContext.hadoopConfiguration
//
//    val targetPath = new org.apache.hadoop.fs.Path(targetFile)
//    val fs         = targetPath.getFileSystem(conf)
//
//    // Ensure parent directory exists
//    Option(targetPath.getParent).foreach { parent =>
//      if (!fs.exists(parent)) fs.mkdirs(parent)
//    }
//
//    // If overwriting, remove any existing file or directory at target
//    if (overwrite && fs.exists(targetPath)) fs.delete(targetPath, true)
//
//    // Write to a temp directory as CSV (Spark always writes a folder)
//    val tmpPath = new org.apache.hadoop.fs.Path(
//      targetPath.getParent,
//      s".metrics_tmp_${System.currentTimeMillis()}"
//    )
//
//    df.coalesce(1)
//      .write
//      .mode(org.apache.spark.sql.SaveMode.Overwrite)
//      .option("header", "true")
//      .csv(tmpPath.toString)
//
//    // Find the single part file that Spark wrote
//    val part = fs.listStatus(tmpPath)
//      .map(_.getPath)
//      .find(p => p.getName.startsWith("part-") && p.getName.endsWith(".csv"))
//      .getOrElse(throw new RuntimeException(s"No CSV part file found in $tmpPath"))
//
//    // Move/rename the part to the exact target file
//    fs.rename(part, targetPath)
//
//    // Clean temp directory
//    fs.delete(tmpPath, true)
//  }
//
//  // ---------- Paths ----------
//  private case class OutPaths(base: String) {
//    val docsPath   = s"$base/doc_normalized"   // versioned (append)
//    val chunksPath = s"$base/chunks"           // versioned (append)
//    val embedsPath = s"$base/embeddings"       // versioned (append)
//    val indexPath  = s"$base/retrieval_index"  // snapshot (overwrite)
//    val manifestPath = s"$base/_manifest"      // last-seen (uri,length,mtime,sha)
//  }
//
//  // ---------- Public entry point ----------
//  /**
//   * @param embedder     logical embedder name (e.g. "mxbai-embed-large")
//   * @param embVersion   your internal version string for the embedder + preprocessing (e.g. "v1")
//   * @param shardBuckets number of shard buckets for partitioning (doc-affinity)
//   */
//  def run(
//           spark: SparkSession,
//           pdfRoot: String,
//           outDir: String,
//           embedder: String   = "mxbai-embed-large",
//           embVersion: String = "v1",
//           shardBuckets: Int  = 64
//         ): Unit = {
//    import spark.implicits._
//    Console.println("\n\n\n\n\n=== Incremental Database Creation Pipeline ===")
//    Console.println(s"Input PDF root: $pdfRoot")
//    Console.println(s"Output directory: $outDir")
//
//    // Normalize root URI once (works for file://, hdfs://, s3://, Windows paths)
//    val rootUriNorm: String =
//      new org.apache.hadoop.fs.Path(pdfRoot).toUri.normalize().toString.stripSuffix("/")
//
//    // Relativize a file URI against root to build a stable docId (forward slashes)
//    def toDocId(fileUri: String): String = {
//      val u = new org.apache.hadoop.fs.Path(fileUri).toUri.normalize().toString
//      val rel =
//        if (u.startsWith(rootUriNorm)) u.substring(rootUriNorm.length).stripPrefix("/")
//        else new org.apache.hadoop.fs.Path(fileUri).getName
//      // Keep the .pdf name; if you prefer without extension, add .stripSuffix(".pdf")
//      rel.replace('\\', '/')
//    }
//
//    // Use existing helper to list PDFs (uri, length, mtime)
//    val statsNow = hdfsListFiles(spark, pdfRoot)
//
//    val docsNowDF = statsNow
//      .map { fs => (toDocId(fs.uri), fs.uri, fs.length, fs.mtime) }
//      .toDF("docId", "uri", "length", "mtime")
//
//    // (Optional) quick sanity log
//    val nDocs = docsNowDF.count()
//    logger.info(s"[Discover] Found $nDocs PDF(s) under $pdfRoot")
//    docsNowDF.show(math.min(nDocs, 5).toInt, truncate = false)
//
//
//
//    Console.println("=== Incremental Database Creation Pipeline END ===\n\n\n\n\n")
//    logger.info(s"[pipeline] Metrics updated: $outDir/_metrics_parquet and $outDir/run_metrics.csv")
//  }
//
//  // ---------- Snapshot index (Step 7) ----------
//  private def buildAndPublishIndex(
//                                    spark: SparkSession,
//                                    paths: OutPaths,
//                                    embedder: String,
//                                    embVersion: String,
//                                    changedDocIds: Array[String]
//                                  ): Unit = {
//    import org.apache.spark.sql.SaveMode
//    import org.apache.spark.sql.functions._
//    import spark.implicits._
//
//    if (changedDocIds.isEmpty) return
//    if (!pathExists(spark, paths.docsPath) || !pathExists(spark, paths.chunksPath)) {
//      logger.warn("[Index] Docs or Chunks do not exist yet. Skipping index build.")
//      return
//    }
//
//    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
//
//    val docs   = spark.read.parquet(paths.docsPath)
//    val chunks = spark.read.parquet(paths.chunksPath)
//
//    val changedDocs   = docs.where(col("docId").isin(changedDocIds:_*))
//    val changedChunks = chunks.where(col("docId").isin(changedDocIds:_*))
//
//    val latestChangedDocs = changedDocs
//      .withColumn("rn",
//        row_number().over(
//          org.apache.spark.sql.expressions.Window
//            .partitionBy($"docId").orderBy($"ingestedAt".desc)
//        )
//      )
//      .where($"rn" === 1)
//      .drop("rn")
//
//    val latestChangedChunks = latestChangedDocs
//      .select($"docId".as("d_id"), $"contentHash".as("d_hash"), $"title", $"language", $"shard")
//      .join(
//        changedChunks.select($"docId".as("c_id"), $"contentHash".as("c_hash"), $"chunkId", $"chunkIx", $"chunkText", $"sectionPath"),
//        $"d_id" === $"c_id" && $"d_hash" === $"c_hash"
//      )
//      .select(
//        $"d_id".as("docId"),
//        $"d_hash".as("contentHash"),
//        $"title", $"language", $"shard",
//        $"chunkId", $"chunkIx", $"sectionPath", $"chunkText"
//      )
//
//    if (!pathExists(spark, paths.embedsPath)) {
//      logger.warn("[Index] Embeddings path not found; nothing to write for changed docs.")
//      return
//    }
//
//    val emb = spark.read.parquet(paths.embedsPath)
//      .where(col("embedder") === embedder && col("embVersion") === embVersion)
//      .where(col("docId").isin(changedDocIds:_*))
//      .select("docId", "chunkId", "embedding", "embDim")
//
//    val changedIndexDF = latestChangedChunks
//      .join(emb, Seq("docId", "chunkId"))
//      .select(
//        $"shard", $"docId", $"contentHash",
//        $"title", $"language",
//        $"chunkId", $"chunkIx", $"sectionPath", $"chunkText",
//        $"embedding", $"embDim"
//      )
//
//    changedIndexDF
//      .repartition(col("shard"), col("docId"))
//      .write
//      .mode(SaveMode.Overwrite)
//      .option("maxRecordsPerFile", 5000)
//      .partitionBy("shard","docId")
//      .parquet(paths.indexPath)
//
//    logger.info(s"[Index] Incrementally updated ${changedDocIds.length} docId(s) at ${paths.indexPath}")
//  }
//
//  // ---------- helpers ----------
//  final case class FileStat(uri: String, length: Long, mtime: Long)
//
//  private def hdfsListFiles(spark: SparkSession, root: String): Vector[FileStat] = {
//    val conf = spark.sparkContext.hadoopConfiguration
//    val fs   = new org.apache.hadoop.fs.Path(root).getFileSystem(conf)
//
//    def listRec(p: org.apache.hadoop.fs.Path): Vector[FileStat] = {
//      val it = fs.listStatusIterator(p)
//      val out = scala.collection.mutable.ArrayBuffer.empty[FileStat]
//      while (it.hasNext) {
//        val st = it.next()
//        if (st.isDirectory) out ++= listRec(st.getPath)
//        else {
//          val uri = st.getPath.toString
//          if (uri.toLowerCase.endsWith(".pdf"))
//            out += FileStat(uri, st.getLen, st.getModificationTime)
//        }
//      }
//      out.toVector
//    }
//
//    listRec(new org.apache.hadoop.fs.Path(root))
//  }
//
//  private def sha256Hex(s: String): String = {
//    val md = java.security.MessageDigest.getInstance("SHA-256")
//    md.update(s.getBytes("UTF-8"))
//    md.digest().map("%02x".format(_)).mkString
//  }
//
//  private def pathExists(spark: SparkSession, path: String): Boolean =
//    try { spark.read.parquet(path).limit(1).count() >= 0 } catch { case _: Throwable => false }
//
//  private def emptyDocsDeltaFrame(spark: SparkSession): DataFrame =
//    spark.createDataFrame(
//      spark.sparkContext.emptyRDD[Row],
//      StructType(Seq(
//        StructField("docId", StringType,   nullable = false),
//        StructField("contentHash", StringType, nullable = false)
//      ))
//    )
//
//  private def emptyEmbKeysFrame(spark: SparkSession): DataFrame =
//    spark.createDataFrame(
//      spark.sparkContext.emptyRDD[Row],
//      StructType(Seq(
//        StructField("docId", StringType,   nullable = false),
//        StructField("chunkId", StringType, nullable = false)
//      ))
//    )
//
//  private def nowMs(): Long = java.time.Instant.now().toEpochMilli
//}
