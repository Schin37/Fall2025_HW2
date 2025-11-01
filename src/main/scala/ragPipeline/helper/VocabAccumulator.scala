package ragPipeline.helper

import scala.collection.mutable

/**
 * VocabAccumulator
 * ----------------
 * Collects and counts term frequencies across all processed chunks.
 * Used to generate a deterministic vocabulary CSV: term, token_id, total_tf
 */
final class VocabAccumulator {

  private val totalTF = mutable.HashMap.empty[String, Long]

  /** Adds all words from a text chunk to the accumulator */
  def addChunk(text: String): Unit = {
    val words = Chunker.splitWords(text)
    words.foreach { w =>
      val next = totalTF.getOrElse(w, 0L) + 1L
      totalTF.put(w, next)
    }
  }


  /** Returns the number of unique terms tracked */
  def size: Int = totalTF.size

  /** True if no terms have been accumulated */
  def isEmpty: Boolean = totalTF.isEmpty

  /**
   * Writes accumulated vocabulary as CSV lines: term, token_id, total_tf.
   * Deterministically ordered by descending frequency then alphabetically.
   */
  def writeCsv(writeLine: String => Unit, includeHeader: Boolean = true): Unit = {
    if (includeHeader) writeLine("term,token_id,total_tf")

    // stable, deterministic order: freq DESC, term ASC
    val sorted: Seq[(String, Long)] =
      totalTF.toSeq.sortBy { case (term, n) => (-n, term) }

    // assign token_id = position in the sorted vocab
    sorted.zipWithIndex.foreach { case ((term, n), id) =>
      writeLine(s"${csvEscape(term)},$id,$n")
    }
  }

  /** Escapes CSV fields by quoting and doubling internal quotes */
  private def csvEscape(s: String): String =
    "\"" + s.replace("\"", "\"\"") + "\""
}
