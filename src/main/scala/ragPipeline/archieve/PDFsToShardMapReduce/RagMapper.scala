package ragPipeline.archieve.PDFsToShardMapReduce

import org.apache.hadoop.io.{BytesWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory
import io.circe.Json
import org.apache.hadoop.fs.{Path => HPath}
import ragPipeline.helper.Chunker
import ragPipeline.helper.Vectors
import ragPipeline.helper.Pdf
import ragPipeline.models.Ollama

/**
 * Mapper stage: one mapper handles one PDF.
 *  - Extracts text
 *  - Splits into chunks
 *  - Embeds chunks using Ollama
 *  - Emits JSON rows keyed by shard ID
 */
class RagMapper extends Mapper[Text, BytesWritable, IntWritable, Text] {

  private val log = LoggerFactory.getLogger(getClass)
  private val client = new Ollama()  // local embedding client

  override def map(
                    key: Text,
                    value: BytesWritable,
                    ctx: Mapper[Text, BytesWritable, IntWritable, Text]#Context
                  ): Unit = {

    val path = new HPath(key.toString)
    val docId = path.getName
    log.info(s"[Mapper] $docId start")

    // Extract PDF text
    val text = Pdf.readTextBytes(value)
    if (text == null || text.trim.isEmpty) {
      log.info(s"[Mapper] $docId empty text; skipping")
      ctx.getCounter("HW1", "PDF_EMPTY").increment(1)
      return
    }

    // Split into chunks
    val chunks = Chunker.split(text)
    Console.println("Ragmapper Chunks: " + chunks)
    if (chunks.isEmpty) {
      ctx.getCounter("HW1", "CHUNK_EMPTY").increment(1)
      return
    }

    // Embed & normalize
    val embeddings = client.embed(chunks, "mxbai-embed-large").map(Vectors.l2Normalization)

    val numReducers = ctx.getNumReduceTasks
    require(numReducers > 0, "Number of reducers must be > 0")
    val shard = math.abs(docId.hashCode) % numReducers

    // Emit JSON rows
    chunks.zip(embeddings).zipWithIndex.foreach {
      case ((chunk, vec), id) =>
        if (vec == null || vec.isEmpty || vec.exists(v => v.isNaN || v.isInfinity)) {
          ctx.getCounter("HW1", "EMPTY_OR_BAD_VEC").increment(1)
        } else {
          val rec = Json.obj(
            "doc_id"   -> Json.fromString(docId),
            "chunk_id" -> Json.fromInt(id),
            "text"     -> Json.fromString(chunk),
            "vec"      -> Json.arr(vec.map(Json.fromFloatOrNull): _*)
          ).noSpaces
          ctx.write(new IntWritable(shard), new Text(rec))
          ctx.getCounter("HW1", "MAP_WRITES").increment(1)
        }
    }

    ctx.getCounter("HW1", "PDFS_DONE").increment(1)
    ctx.getCounter("HW1", "CHUNKS_EMITTED").increment(chunks.size)
    log.info(s"[Mapper] $docId chunks=${chunks.size}")
  }
}
