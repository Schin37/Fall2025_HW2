package ragPipeline.IncrementalDatabaseCreation

import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import ragPipeline.IncrementalDatabaseCreation.IncrementalDatabasePipeline.FileStat

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.security.MessageDigest

object IncrementalHelper extends Serializable{
  def pathExistsHadoop(spark: SparkSession, uri: String): Boolean = {
    val p  = new org.apache.hadoop.fs.Path(uri)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(p)
  }

  def hdfsListFiles(spark: SparkSession, root: String): Vector[FileStat] = {
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

  def sha256Hex(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.update(s.getBytes("UTF-8"))
    md.digest().map("%02x".format(_)).mkString
  }

  def pathExists(spark: SparkSession, path: String): Boolean =
    try { spark.read.parquet(path).limit(1).count() >= 0 } catch { case _: Throwable => false }

  def emptyDocsDeltaFrame(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("docId", StringType,   nullable = false),
        StructField("contentHash", StringType, nullable = false)
      ))
    )

  def emptyEmbKeysFrame(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("docId", StringType,   nullable = false),
        StructField("chunkId", StringType, nullable = false)
      ))
    )

  def nowMs(): Long = java.time.Instant.now().toEpochMilli


  // Safer hex for Java 8/11; avoids java.util.HexFormat
  def bytesToHex(bytes: Array[Byte]): String =
    bytes.map("%02x".format(_)).mkString

  //  def read(b: Array[Byte]): Int

  def fileSize(uri: String)(implicit spark: SparkSession): Long = {
    val conf = spark.sparkContext.hadoopConfiguration
    val p    = new Path(uri)
    val fs   = p.getFileSystem(conf)
    fs.getFileStatus(p).getLen   // length in bytes
  }

  // Hadoop-friendly SHA-256 over any Hadoop-supported URI (file://, hdfs://, s3a://)
  def sha256OfUri(uri: String)(implicit spark: SparkSession): String = {
    if (uri == null || uri.isEmpty)
      throw new IllegalArgumentException("sha256OfUri: uri is null or empty")

    val sc = spark.sparkContext
    if (sc == null) throw new IllegalStateException("sha256OfUri: sparkContext is null")

    val conf = sc.hadoopConfiguration
    if (conf == null) throw new IllegalStateException("sha256OfUri: Hadoop conf is null")

    val path = new org.apache.hadoop.fs.Path(uri)
    val fs   = path.getFileSystem(conf)
    if (fs == null) throw new IllegalStateException(s"sha256OfUri: FileSystem is null for uri=$uri")

    if (!fs.exists(path))
      throw new java.io.FileNotFoundException(s"sha256OfUri: missing file: $uri")

    var in: org.apache.hadoop.fs.FSDataInputStream = null
    try {
      in = fs.open(path) // could throw for odd Windows URIs; we’ll wrap below
      val md  = java.security.MessageDigest.getInstance("SHA-256")
      val buf = new Array[Byte](64 * 1024)

      var n = in.read(buf)
      while (n != -1) {
        if (n > 0) md.update(buf, 0, n)
        n = in.read(buf)
      }
      bytesToHex(md.digest())
    } catch {
      case npe: NullPointerException =>
        // wrap with URI so the log points to the exact offender
        throw new RuntimeException(s"sha256OfUri NPE for uri=$uri", npe)
    } finally {
      if (in != null) in.close()
    }
  }

  /** Write df to a temp dir and atomically swap it into targetDir.
   * Avoids the “self-overwrite while reading” Spark race.
   */
  def atomicReplaceParquet(df: DataFrame, targetDir: String): Unit = {
    val spark = df.sparkSession
    val conf  = spark.sparkContext.hadoopConfiguration

    val target = new Path(targetDir)
    val fs     = target.getFileSystem(conf)

    // Ensure parent exists
    val parent = Option(target.getParent).getOrElse(new Path("/"))
    if (!fs.exists(parent)) fs.mkdirs(parent)

    // Write to a unique temp folder
    val tmp = new Path(parent, s".swap_${System.currentTimeMillis()}")

    // If this dataset is small and you want one file, you can coalesce(1) here.
    // df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(tmp.toString)
    df.write.mode(SaveMode.Overwrite).parquet(tmp.toString)

    // Replace target with the fresh temp
    if (fs.exists(target)) fs.delete(target, true)
    fs.rename(tmp, target)

    // Make sure Spark doesn't hold stale listings
    spark.catalog.refreshByPath(targetDir)
    spark.catalog.clearCache()
  }


  def readAllBytes(fsIn: FSDataInputStream, len: Long): Array[Byte] = {
    val buf = new Array[Byte](64 * 1024)
    val out = new java.io.ByteArrayOutputStream(math.min(len, 1000000L).toInt.max(64*1024))
    try {
      var n = fsIn.read(buf)
      while (n != -1) { if (n > 0) out.write(buf, 0, n); n = fsIn.read(buf) }
      out.toByteArray
    } finally out.close()
  }

  def sha256HexString(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    val bytes = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    bytes.map("%02x".format(_)).mkString
  }

  def firstLineOrName(text: String, uri: String): String = {
    val first = text.linesIterator.map(_.trim).find(_.nonEmpty).getOrElse("")
    if (first.nonEmpty) first.take(200) else new Path(uri).getName.stripSuffix(".pdf")
  }

  def detectLangCheap(s: String): String = {
    val ascii = if (s.isEmpty) 1.0 else s.count(_ < 128).toDouble / s.length
    if (ascii > 0.95) "en" else "unk"
  }
}


object EmbCache extends Serializable{
  private val root = Paths.get("./emb-cache")
  Files.createDirectories(root)

  private def toHex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    val hex = "0123456789abcdef".toCharArray
    var i = 0
    while (i < bytes.length) {
      val b = bytes(i) & 0xff
      sb.append(hex(b >>> 4))
      sb.append(hex(b & 0x0f))
      i += 1
    }
    sb.toString()
  }

  def sha256(s: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    toHex(md.digest(s.getBytes("UTF-8")))
  }

  def get(text: String): Option[Array[Float]] = {
    val key = sha256(text)
    val p   = root.resolve(s"$key.vec")
    if (Files.exists(p)) {
      val bytes = Files.readAllBytes(p)
      val fb    = ByteBuffer.wrap(bytes).asFloatBuffer()
      val arr   = new Array[Float](fb.remaining())
      fb.get(arr)
      Some(arr)
    } else None
  }

  def put(text: String, vec: Array[Float]): Unit = {
    val key = sha256(text)
    val p   = root.resolve(s"$key.vec")
    val bb  = ByteBuffer.allocate(vec.length * 4)
    bb.asFloatBuffer().put(vec)
    Files.write(p, bb.array(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}
