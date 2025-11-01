package ragPipeline.helper

import org.apache.lucene.document.Document
import org.apache.lucene.search.KnnFloatVectorQuery
import org.slf4j.LoggerFactory

import ragPipeline.archieve.PDFsToShardMapReduce
import ragPipeline.models.Ollama
import java.nio.file.{Path => JPath}

/**
 * QueryPipeline
 * --------------
 * Purpose:
 * Handles the query-time logic of the RAG pipeline:
 *  - Embeds the user's question using the same embedding model used for the corpus.
 *  - Performs a similarity (k-NN) search in the Lucene vector index.
 *  - Returns the top-K most relevant text chunks.
 *
 * Functions included:
 *  1) getRelevantCompressedData — embeds and searches for relevant chunks.
 *  2) decompress — unpacks Lucene Documents into readable text tuples for display or generation.
 */
object QueryPipeline {

  private val log = LoggerFactory.getLogger(getClass)
  private val client = new Ollama() // gets instance of client to use the model

  /**
   * getRelevantCompressedData
   * --------------------------
   * Purpose:
   * Given a question, this method embeds it into a vector, performs a k-NN search,
   * and returns the top-K Lucene documents with their similarity scores.
   */
  def getRelevantConpressedData(
                                 Question: String,
                                 path: JPath,
                                 columnsWanted: Set[String],
                                 topK: Int
                               ): Seq[(Float, Document)] = {

    // Step 1: normalize the question
    val wantedColumns = columnsWanted
    val normalizedQuestion = Chunker.normalize(Question)

    // Step 2: prepare text for embedding
    val vectorQuestion = Vector(normalizedQuestion)

    // Step 3: embed and normalize
    val embeddedVectorQuestion =
      client.embed(vectorQuestion).map(Vectors.l2Normalization).head

    // Step 4: build Lucene query
    val query = new KnnFloatVectorQuery("vec", embeddedVectorQuestion, topK)

    // Step 5: run search and return results
    val topDocuments: Seq[(Float, Document)] =
      Searcher.search(query, topK, path, wantedColumns)

    topDocuments
  }

  /**
   * decompress
   * -----------
   * Converts compact Lucene document results into plain text tuples.
   */
  def decomPress(
                  compressedData: Seq[(Float, Document)]
                ): Seq[(Float, String, String, String)] = {
    compressedData.map {
      case (score, doc) =>
        val text = Option(doc.get("text")).getOrElse("")
        val src = Option(doc.get("doc_id")).getOrElse("")
        val chunkId = Option(doc.get("chunk_id")).getOrElse("")
        log.info(s"[MapperRI] SAMPLE text($chunkId): " + text.take(120).replace("\n", " "))
        (score, text, src, chunkId)
    }
  }

}
