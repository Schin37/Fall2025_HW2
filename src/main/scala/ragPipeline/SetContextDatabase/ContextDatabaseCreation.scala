package ragPipeline.SetContextDatabase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path => HPath, PathFilter}
import org.apache.hadoop.io.{BytesWritable, IntWritable, Text}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import ragPipeline.IncrementalDatabaseCreation.IncrementalDatabasePipeline
import ragPipeline.archieve.PDFsToShardMapReduce.{RagMapper, ShardReducer}
import ragPipeline.config.AppConfig

import scala.annotation.tailrec
import scala.util.Try

object ContextDatabaseCreation {

  private val log = LoggerFactory.getLogger(getClass)

  private val Usage: String =
    """Usage: add-context [--pdfRoot <dir>] [--outDir <dir>] [--reducers N] [--engine spark|mr]
      |       Defaults come from application.conf when not provided.
      |""".stripMargin.trim

  private final case class CliOptions(
    pdfRoot: String = AppConfig.paths.pdfRoot.toString,
    outDir: String = AppConfig.paths.outDir.toString,
    reducers: Int = AppConfig.mr.reducers,
    engineOverride: Option[String] = None
  )

  def run(args: Array[String]): Unit = {
    parseArgs(args.toList) match {
      case Left(err) =>
        Console.err.println(err)
        Console.err.println(Usage)
        ()
      case Right(opts) =>
        val engine = opts.engineOverride.getOrElse(AppConfig.engine.engine).toLowerCase
        engine match {
          case "spark" => runSpark(opts)
          case "mr"    => runMapReduce(opts)
          case other =>
            log.warn(s"Unknown engine '$other'; defaulting to Spark incremental builder.")
            runSpark(opts)
        }
    }
  }

  @tailrec
  private def parseArgs(args: List[String], acc: CliOptions = CliOptions()): Either[String, CliOptions] = {
    args match {
      case Nil => Right(acc)
      case flag :: _ if flag == "--help" || flag == "-h" =>
        Left(Usage)
      case ("--pdfRoot" | "--pdfroot" | "--in" | "--input") :: value :: tail =>
        parseArgs(tail, acc.copy(pdfRoot = value))
      case ("--outDir" | "--outdir" | "--out" | "--output") :: value :: tail =>
        parseArgs(tail, acc.copy(outDir = value))
      case ("--reducers" | "--shards" | "--numReducers") :: value :: tail =>
        val parsed = Try(value.toInt).toOption.filter(_ > 0)
        parsed match {
          case Some(r) => parseArgs(tail, acc.copy(reducers = r))
          case None    => Left(s"Invalid reducer count: '$value'")
        }
      case ("--engine" | "--mode") :: value :: tail =>
        parseArgs(tail, acc.copy(engineOverride = Some(value)))
      case flag :: Nil if flag.startsWith("--") =>
        Left(s"Option '$flag' requires a value")
      case unknown :: tail =>
        log.warn(s"Ignoring unrecognized argument '$unknown'")
        parseArgs(tail, acc)
    }
  }

  private def runSpark(opts: CliOptions): Unit = {
    val spark = SparkSession.builder()
      .appName("RAG Context Builder (Spark)")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      val embedder = s"ollama:${AppConfig.models.embedModel}"
      IncrementalDatabasePipeline.run(
        spark,
        opts.pdfRoot,
        opts.outDir,
        embedder = embedder,
        embVersion = AppConfig.embed.embVersion,
        shardBuckets = AppConfig.chunking.NumShardBuckets
      )
    } finally {
      spark.stop()
    }
  }

  private def runMapReduce(opts: CliOptions): Unit = {
    val conf = new Configuration()
    AppConfig.writeTo(conf)

    val inputPath  = new HPath(opts.pdfRoot)
    val outputPath = new HPath(opts.outDir)

    val fs = inputPath.getFileSystem(conf)
    require(fs.exists(inputPath), s"Input path does not exist: ${inputPath.toString}")

    val outFs = outputPath.getFileSystem(conf)
    if (outFs.exists(outputPath)) {
      if (AppConfig.engine.deleteOutputIfExists) {
        log.info(s"Deleting existing output at ${outFs.makeQualified(outputPath)}")
        outFs.delete(outputPath, true)
      } else {
        throw new IllegalStateException(s"Output path already exists: ${outputPath.toString}")
      }
    }

    val job = Job.getInstance(conf, s"RAG Context Builder (${opts.reducers} reducers)")
    job.setJarByClass(classOf[RagMapper])

    FileInputFormat.addInputPath(job, inputPath)
    FileInputFormat.setInputDirRecursive(job, true)
    FileInputFormat.setInputPathFilter(job, classOf[PdfOnlyPathFilter])
    job.setInputFormatClass(classOf[PdfWholeFileInputFormat])

    job.setMapperClass(classOf[RagMapper])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setReducerClass(classOf[ShardReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    job.setNumReduceTasks(math.max(1, opts.reducers))

    FileOutputFormat.setOutputPath(job, outputPath)

    log.info(s"Starting MapReduce context build: input=$inputPath output=$outputPath reducers=${job.getNumReduceTasks}")
    val ok = job.waitForCompletion(true)
    if (!ok) throw new RuntimeException("Context MapReduce job failed")
  }
}

private final class PdfWholeFileInputFormat extends FileInputFormat[Text, BytesWritable] {
  override def isSplitable(ctx: JobContext, file: HPath): Boolean = false
  override def createRecordReader(split: InputSplit, ctx: TaskAttemptContext): RecordReader[Text, BytesWritable] =
    new PdfWholeFileRecordReader
}

private final class PdfOnlyPathFilter extends PathFilter {
  private val conf = new Configuration()
  override def accept(path: HPath): Boolean = {
    val fs = path.getFileSystem(conf)
    fs.isDirectory(path) || path.getName.toLowerCase.endsWith(".pdf")
  }
}

private final class PdfWholeFileRecordReader extends RecordReader[Text, BytesWritable] {
  private var key: Text = _
  private var value: BytesWritable = _
  private var processed = false
  private var split: org.apache.hadoop.mapreduce.lib.input.FileSplit = _
  private var in: FSDataInputStream = _

  override def initialize(givenSplit: InputSplit, ctx: TaskAttemptContext): Unit = {
    split = givenSplit.asInstanceOf[org.apache.hadoop.mapreduce.lib.input.FileSplit]
    key = new Text(split.getPath.toString)
    val fs = split.getPath.getFileSystem(ctx.getConfiguration)
    in = fs.open(split.getPath)
  }

  override def nextKeyValue(): Boolean = {
    if (processed) return false
    val lenL = split.getLength
    if (lenL > Int.MaxValue)
      throw new IllegalArgumentException(s"File too large for single mapper (${lenL} bytes): ${split.getPath}")
    val len = lenL.toInt
    val buf = new Array[Byte](len)
    IOUtils.readFully(in, buf, 0, len)
    value = new BytesWritable(buf)
    processed = true
    true
  }

  override def getCurrentKey: Text = key
  override def getCurrentValue: BytesWritable = value
  override def getProgress: Float = if (processed) 1f else 0f
  override def close(): Unit = if (in != null) in.close()
}
