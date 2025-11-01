package ragPipeline.Query


// Libraries to extract text from pdf

import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, FileUtil, PathFilter, Path => HPath}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}

import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.slf4j.LoggerFactory

import ragPipeline.helper.{Chunker, Pdf, Searcher, Vectors, ExtractFinal}
import ragPipeline.RelevantInfoMapReduce.{RagMapperRelevantInfo, ShardReducerRelevantInfo}
import ragPipeline.models.OllamaJson.ChatMessage
import ragPipeline.models.{Ask, Ollama}


// Import a small helper to auto-close resources (like PDDocument) safely

/*
1) extract pdf text and convert it to a text file
2) read text file and chunk (chuck into a vector of Strings)
3) take chunked text to ollam to get embedded
 */

object QueryEngineMapReducer{
  private val log = LoggerFactory.getLogger(getClass)
  private val DefaultTopK = 5
  private val RankDirPrefix = "_rank-"
  def run(args: Array[String]): Unit = {


    if (args.length < 2) {
      Console.err.println(
        "Usage: QueryMain <shardsParentDir> <question ...>\n" +
          "Example: QueryMain /data/hw1-out 'What does MSR say about bug prediction?'"
      )
      sys.exit(1)
    }

    val shardsParentDir = new HPath(args(0))
    val question = args.drop(1).mkString(" ").trim
    val topK = sys.props.get("rag.topK").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(DefaultTopK)


    // ---- 1) Configure MR job ----
    val conf = new Configuration()
    conf.set("rag.question", question)
    conf.setInt("rag.topK", topK)
    conf.set("rag.shards.root", shardsParentDir.toString)

    conf.set("rag.question", question)
    conf.set("rag.shardParentDirectory", shardsParentDir.toString)
    conf.setStrings("rag.wanted.columns", "doc_id", "chunk_id", "text", "source")
    conf.setInt("rag.topK", 5)


    val job = Job.getInstance(conf, s"RAG Query TopK=$topK")
    job.setJarByClass(classOf[RagMapperRelevantInfo]) // your mapper class jar anchor

    // Input: 1 record per shard (point at a file guaranteed to exist in each shard)
    // We use segments_* file to ensure each shard yields exactly one map task.
    FileInputFormat.addInputPath(job, new HPath(shardsParentDir, "index_shard_*/segments_*"))
    FileInputFormat.setInputDirRecursive(job, true)
    FileInputFormat.setInputPathFilter(job, classOf[LuceneSegmentsFilter]) // your filter
    job.setInputFormatClass(classOf[WholeFileInputFormat]) // your 1-record-per-file input

    // Mapper / Reducer
    job.setMapperClass(classOf[RagMapperRelevantInfo])
    job.setReducerClass(classOf[ShardReducerRelevantInfo])
    job.setMapOutputKeyClass(classOf[org.apache.hadoop.io.IntWritable])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(1) // single global top-k JSON


    // ==== Output dir policy: write to temp, then swap into fixed "content-ranking" ====
    job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]]) // explicit

    val parentDir = shardsParentDir // e.g., C:/tmp/hw1-out
    val finalDir = new HPath(parentDir, "content-ranking") // single stable folder
    val fs = parentDir.getFileSystem(conf)

    // ensure parent exists; DO NOT touch parent
    fs.mkdirs(parentDir)

    // Hadoop requires output path NOT to exist
    if (fs.exists(finalDir)) {
      Console.println(s"[SAFE-DELETE] Removing only: ${fs.makeQualified(finalDir)}")
      fs.delete(finalDir, true) // delete ONLY the child
    }

    FileOutputFormat.setOutputPath(job, finalDir)

    // run + show results
    val ok = job.waitForCompletion(true)
    if (!ok) sys.error("Query MR job failed")







    //    Console.println(s"[QueryMain] Output dir: " + fs.makeQualified(finalDir))
    fs.listStatus(finalDir).foreach(st => Console.println(" - " + st.getPath))

    val context = ExtractFinal.readContextFromDir(fs.makeQualified(finalDir).toString, conf)
    //    Console.println("\n=== CONTEXT ===\n" + context)

    val client = new Ollama()

    val messages: Vector[ChatMessage] = Ask.buildMessages(question, context)
    val answer = client.chat(messages, "llama3.1:8b")
    Console.println(answer)


    System.exit(0)








  }
}
  












  // ################################################################################################
  // ################################################################################################

  // Accept Lucene index marker files inside each shard dir
  class LuceneSegmentsFilter extends PathFilter {
    override def accept(p: HPath): Boolean =
      p.getName.startsWith("segments_") // e.g., /…/<shard>/segments_*
  }

  class WholeFileInputFormat extends FileInputFormat[Text, BytesWritable] {
    override def isSplitable(ctx: JobContext, file: HPath): Boolean = false
    override def createRecordReader(split: InputSplit, ctx: TaskAttemptContext)
    : RecordReader[Text, BytesWritable] = new WholeFileRecordReader
  }

  class WholeFileRecordReader extends RecordReader[Text, BytesWritable] {
    private var key: Text = _
    private var value: BytesWritable = _
    private var processed = false
    private var split: FileSplit = _
    private var in: FSDataInputStream = _

    override def initialize(givenSplit: InputSplit, ctx: TaskAttemptContext): Unit = {
      split = givenSplit.asInstanceOf[FileSplit]
      key   = new Text(split.getPath.toString)
      val fs = split.getPath.getFileSystem(ctx.getConfiguration)
      in = fs.open(split.getPath)
    }

    override def nextKeyValue(): Boolean = {
      if (processed) return false
      val len = split.getLength.toInt
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