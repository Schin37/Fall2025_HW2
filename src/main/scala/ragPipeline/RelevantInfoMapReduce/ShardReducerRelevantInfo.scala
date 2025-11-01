package ragPipeline.RelevantInfoMapReduce

import io.circe.parser._
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

/** Reducer that merges all shard hits and keeps only the Top-K overall. */
class ShardReducerRelevantInfo
  extends Reducer[IntWritable, Text, NullWritable, Text] {

  private val log = LoggerFactory.getLogger(getClass)

  // --- simple case class for parsed rows ---
  case class Hit(score: Double, docId: String, chunkId: Int, source: String, text: String)

  override def reduce(
                       key: IntWritable,
                       values: java.lang.Iterable[Text],
                       ctx: Reducer[IntWritable, Text, NullWritable, Text]#Context
                     ): Unit = {

    val conf      = ctx.getConfiguration
    val topK      = Option(conf.get("rag.topK")).flatMap(s => Try(s.toInt).toOption).getOrElse(5)
    val question  = conf.get("rag.question", "")

    // Min-heap by score (so smallest score is dropped first when >K)
    implicit val byMinScore: Ordering[Hit] =
      Ordering.by[Hit, (Double, String, Int)](h => (h.score, h.docId, h.chunkId))
    val heap = mutable.PriorityQueue.empty[Hit](byMinScore.reverse)

    // ---- Parse and accumulate hits ----
    values.forEach { t =>
      val json = t.toString
      try {
        parse(json) match {
          case Left(err) =>
            ctx.getCounter("HW1", "RI_BAD_JSON").increment(1)
            log.warn(s"[Reducer] bad JSON: ${err.getMessage} :: ${json.take(150)}")

          case Right(js) =>
            val c = js.hcursor
            val parsed = for {
              docId  <- c.get[String]("doc_id").toOption
              chunk  <- c.get[Int]("chunk_id").toOption
              score  <- c.get[Double]("score").toOption
              source <- c.get[String]("source").toOption
              text   <- c.get[String]("text").toOption
            } yield Hit(score, docId, chunk, source, text)

            parsed match {
              case Some(hit) =>
                heap.enqueue(hit)
                if (heap.size > topK) heap.dequeue()
                ctx.getCounter("HW1", "RI_GOOD").increment(1)
              case None =>
                ctx.getCounter("HW1", "RI_BAD_FIELDS").increment(1)
                log.info(s"[Reducer] Missing fields: ${json.take(150)}")
            }
        }
      } catch {
        case e: Throwable =>
          ctx.getCounter("HW1", "RI_UNCAUGHT").increment(1)
          log.error(s"[Reducer] ${e.getClass.getSimpleName}: ${e.getMessage}")
      }
    }

    // ---- Emit final JSON results ----
    val top = heap.dequeueAll.sortBy(h => (-h.score, h.docId, h.chunkId))

    def q(s: String): String =
      "\"" + s.flatMap {
        case '\\' => "\\\\"
        case '"'  => "\\\""
        case '\n' => "\\n"
        case '\r' => "\\r"
        case '\t' => "\\t"
        case c if c.isControl => f"\\u${c.toInt}%04x"
        case c => c.toString
      } + "\""

    val hitsJson = top.map { h =>
      s"""{"score":${h.score},"doc_id":${q(h.docId)},"chunk_id":${h.chunkId},"source":${q(h.source)},"text":${q(h.text)}}"""
    }.mkString("[", ",", "]")

    val outJson = s"""{"question":${q(question)},"topK":$topK,"hits":$hitsJson}"""
    ctx.write(NullWritable.get(), new Text(outJson))
    ctx.getCounter("HW1", "RI_WRITES").increment(1)

    // Optional “context” output for readability
    val contextText = top.map { h =>
      val head =
        if (h.source.nonEmpty) f"[${h.source} #${h.chunkId} | score=${h.score}%.4f]"
        else f"[${h.docId} #${h.chunkId} | score=${h.score}%.4f]"
      s"$head\n${h.text}"
    }.mkString("\n\n")

    ctx.write(NullWritable.get(), new Text(s"""{"context":${q(contextText)}}"""))
    ctx.getCounter("HW1", "RI_CONTEXT_WRITES").increment(1)
  }
}
