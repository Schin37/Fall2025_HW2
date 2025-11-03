package ragPipeline.Query

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ragPipeline.models.OllamaJson.ChatMessage
import ragPipeline.models.{Ask, Ollama}
import ragPipeline.config.AppConfig

import scala.util.Try
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import ragPipeline.config.AppConfig.query.perChunkCap

/** Spark-based RAG query that reads the HW2 incremental snapshot.
 *
 * The class performs three logical stages:
 *   1. Resolve and load the published retrieval index (table or Parquet path).
 *   2. Embed the incoming question and score cosine similarity against chunk embeddings.
 *   3. Stitch the top-K chunks into a context string before calling the chat model.
 *
 * Priority for locating the retrieval snapshot:
 *   1) -Drag.indexTable=rag.retrieval_index
 *   2) -Drag.indexPath=/abs/or/relative/path/to/out/retrieval_index
 *   3) AppConfig.paths.retrievalIndexDir (application.conf)
 *   4) Inferred from first arg (e.g., "out" -> "out/retrieval_index")
 *
 * Back-compat with old launcher:
 *   - If args has 1 item => it's the question.
 *   - If args has 2+ items => FIRST is shards dir (for inference), LAST is the question.
 */
object QueryMain {

  private val log = LoggerFactory.getLogger(getClass)
  private val DefaultTopK = 5

  def run(args: Array[String]): Unit = {
    if (args.isEmpty) {
      System.err.println(
        "Usage: QueryMain <question ...>\n" +
          "Back-compat: if two+ args are given, the FIRST is the shards dir (e.g., out) and the LAST is the question."
      )
      System.exit(1)
    }

    // If 2+ args: first is shardsParent, last is the question. If 1 arg: it's the question.
    val shardsParentOpt = if (args.length >= 2) Some(args.head.trim) else None
    val question        = args.last.trim

    val topK     = sys.props.get("rag.topK").flatMap(s => Try(s.toInt).toOption).getOrElse(DefaultTopK)
    val idxTable = sys.props.get("rag.indexTable")
    val idxPath  = sys.props.get("rag.indexPath")

    val embedModel = sys.props.getOrElse("rag.embedModel", AppConfig.models.embedModel)
    val chatModel  = sys.props.getOrElse("rag.chatModel",  AppConfig.models.chatModel)

    // Query mode is intentionally light-weight: build a single Spark session per
    // invocation so we can leverage Spark SQL for vector scoring without a
    // long-lived cluster.
    val spark = SparkSession.builder()
      .appName(s"RAG Query (Spark) topK=$topK")
      .master(sys.props.getOrElse("spark.master", "local[*]")) // default local mode
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    import spark.implicits._

    // Helper: does path exist?
    def fsExists(p: String): Boolean = {
      if (p == null || p.isEmpty) false
      else {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        try fs.exists(new HPath(p)) catch { case _: Throwable => false }
      }
    }
    def firstExisting(paths: Seq[String]): Option[String] = paths.find(fsExists)

    // 3) Config default from AppConfig
    val cfgIndexOpt: Option[String] =
      Option(AppConfig.paths.retrievalIndexDir).map(_.toString).filter(fsExists)

    // 4) Infer from shards dir (Windows + POSIX variants)
    val inferredOpt: Option[String] = shardsParentOpt.flatMap { base =>
      firstExisting(Seq(
        s"$base/retrieval_index",
        s"$base\\retrieval_index",
        s"$base/out/retrieval_index",
        s"$base\\out\\retrieval_index"
      ))
    }

    // ---- 1) Load the published snapshot (priority order) ----
    // Allow multiple ways to point at the retrieval snapshot to make the CLI
    // easy to use across local and remote environments.
    val baseDF: DataFrame = (idxTable, idxPath, cfgIndexOpt, inferredOpt) match {
      case (Some(t), _, _, _) =>
        println(s"[Query] Reading table: $t")
        spark.table(t)

      case (None, Some(p), _, _) =>
        println(s"[Query] Reading parquet path (prop): $p")
        spark.read.parquet(p)

      case (None, None, Some(p), _) =>
        println(s"[Query] Reading parquet path (config): $p")
        spark.read.parquet(p)

      case (None, None, None, Some(p)) =>
        println(s"[Query] Reading inferred parquet path: $p")
        spark.read.parquet(p)

      case _ =>
        System.err.println(
          "ERROR: Could not find the retrieval snapshot.\n" +
            "Provide one of:\n" +
            "  -Drag.indexTable=<db.table>\n" +
            "  -Drag.indexPath=<parquet dir>\n" +
            "Or set paths.retrievalIndexDir in application.conf\n" +
            "Or pass a shards dir as the first arg (e.g., 'out') that contains 'retrieval_index/'."
        )
        spark.stop(); System.exit(2); return
    }

    val df = baseDF.select(
      $"docId", $"title", $"language",
      $"chunkId", $"chunkIx", $"sectionPath", $"chunkText",
      $"embedding" // Array[Float]
    )

    // ---- 2) Embed the question (Vector[String]) and broadcast ----
    val ollama = new Ollama()
    // Embed the user question once and broadcast the vector so the cosine UDF
    // can reuse it for every chunk in the DataFrame.
    val qEmbeds: Vector[Array[Float]] = ollama.embed(Vector(question), embedModel)
    val qVecF: Array[Float] = qEmbeds.headOption.getOrElse(Array.emptyFloatArray)
    if (qVecF.isEmpty) {
      System.err.println("ERROR: empty question embedding; check Ollama and embed model.")
      spark.stop(); System.exit(3); return
    }
    val qVec = qVecF.map(_.toDouble)
    val qB   = spark.sparkContext.broadcast(qVec)

    val toDouble = udf((xs: Seq[Float]) => if (xs == null) null else xs.map(_.toDouble))
    val cosSim = udf { (v: Seq[Double]) =>
      if (v == null) 0.0
      else {
        val q = qB.value
        val n = math.min(q.length, v.length)
        var i = 0; var dot = 0.0; var qn = 0.0; var vn = 0.0
        while (i < n) { val a = q(i); val b = v(i); dot += a*b; qn += a*a; vn += b*b; i += 1 }
        if (qn == 0.0 || vn == 0.0) 0.0 else dot / (math.sqrt(qn) * math.sqrt(vn))
      }
    }

    // Score each chunk by cosine similarity, trim text to keep the prompt
    // bounded, and keep only the best matches before collecting to the driver.
    val scored =
      df.withColumn("embedding_d", toDouble(col("embedding")))
        .withColumn("score", cosSim(col("embedding_d")))
        // normalize & trim text BEFORE driver collect
        .withColumn("chunkText_norm", regexp_replace(col("chunkText"), "\\s+", " "))
        .withColumn("chunkText_half", expr(s"substring(chunkText_norm, 1, $perChunkCap)"))
        .orderBy(col("score").desc)
        .limit(topK)
        .cache()

    val top = scored.select(
        $"docId",
        $"title",
        $"sectionPath",
        $"chunkText_half".as("chunkText"),
        $"score"
      )
      .as[(String, String, String, String, Double)]
      .collect()
      .toVector

    // ---- 3) Build context and query the chat model ----
    val context =
      top.zipWithIndex.map { case ((docId, title, section, text, s), i) =>
        val head = f"[$i] $title — $section (docId=$docId, score=$s%.4f)"
        s"$head\n$text"
      }.mkString("\n\n---\n\n")

    log.info(s"LLM sizes: question=${question.length}, context=${context.length}")
    Console.println(s"Question=$question\n---\nContext Preview:\n$context")

    val messages: Vector[ChatMessage] = Ask.buildMessages(question, context)
    val answer = ollama.chat(messages, chatModel)

    Console.println("\n=== MODEL ANSWER ===\n" + answer + "\n=== MODEL ANSWER ===\n" )

    spark.stop()
  }
}
