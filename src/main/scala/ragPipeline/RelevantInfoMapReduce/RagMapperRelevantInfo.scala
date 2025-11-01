package ragPipeline.RelevantInfoMapReduce

import org.apache.hadoop.io.{BytesWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.fs.{Path => HPath}
import org.slf4j.LoggerFactory
import ragPipeline.helper.QueryPipeline
import ragPipeline.models.Ollama

import java.nio.file.Paths
import scala.util.Try
import scala.collection.JavaConverters._

/**
 * One mapper is assigned per Lucene shard (directory).
 * It extracts the Top-K relevant chunks for a given question
 * and emits them as JSON for the reducer.
 */
class RagMapperRelevantInfo
  extends Mapper[Text, BytesWritable, IntWritable, Text] {

  private val log = LoggerFactory.getLogger(getClass)

  override def map(
                    key: Text,
                    value: BytesWritable,
                    ctx: Mapper[Text, BytesWritable, IntWritable, Text]#Context
                  ): Unit = {

    val conf = ctx.getConfiguration
    val question = conf.get("rag.question", "")
    val topK = Option(conf.get("rag.topK")).flatMap(s => Try(s.toInt).toOption).getOrElse(5)
    val wantedColumns =
      Option(conf.getStrings("rag.wanted.columns"))
        .map(_.toSet)
        .getOrElse(Set.empty[String])

    // Path to current shard
    val shardPath = new HPath(key.toString)
    val shardDir = shardPath.getParent
    val shardName = if (shardDir != null) shardDir.getName else "unknown"

    log.info(s"[Mapper] Starting shard=$shardName dir=$shardDir")

    // Try to decompress and score top-K chunks
    val decompressedTopK: Seq[(Float, String, String, String)] = try {
      val got =
        QueryPipeline.getRelevantConpressedData(
          question,
          Paths.get(shardDir.toUri),
          wantedColumns,
          topK
        )
      QueryPipeline.decomPress(got)
    } catch {
      case t: Throwable =>
        ctx.getCounter("HW1", "QUERY_PIPELINE_ERROR").increment(1)
        log.error(s"[Mapper] Query failed for shard=$shardName", t)
        Seq.empty
    }

//    // Encode safely for JSON
    def enc(s: String): String =
      "\"" + s
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\r", "\\r")
        .replace("\n", "\\n") + "\""

//    def enc(s: String): String =
//      "\"" + s.flatMap {
//        case '\\' => "\\\\"
//        case '"'  => "\\\""
//        case '\n' => "\\n"
//        case '\r' => "\\r"
//        case '\t' => "\\t"
//        case c if c.isControl => f"\\u${c.toInt}%04x" // escape ALL control chars < 0x20
//        case c => c.toString
//      } + "\""

    def clip(s: String, max: Int = 2000): String =
      if (s.length <= max) s else s.take(max) + "..."

    val shardId = 0 // single global reducer for now

    decompressedTopK.foreach {
      case (score, text, src, chunkId) =>
        val chunkNum = Try(chunkId.toInt).getOrElse(-1)
        val json =
          s"""{"doc_id":${enc(shardName)},"chunk_id":$chunkNum,"score":$score,"source":${enc(src)},"text":${enc(clip(text))}}"""
        Console.println("mapper context viewer: " + json)
        ctx.write(new IntWritable(shardId), new Text(json))
    }

    ctx.getCounter("HW1", "RI_MAP_EMIT").increment(decompressedTopK.size)
    log.info(s"[Mapper] Finished shard=$shardName emitted=${decompressedTopK.size}")
  }
}
