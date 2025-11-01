package ragPipeline.helper

import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, KnnFloatVectorQuery, TopDocs}
import org.apache.lucene.store.FSDirectory

import scala.collection.JavaConverters._
import java.nio.file.Path

/**
 * Searcher
 * --------
 * Handles Lucene vector search queries (k-NN)
 * and returns (score, document) pairs.
 */
object Searcher {

  def search(
              query: KnnFloatVectorQuery,
              cutOff: Int,
              path: Path,
              fields: Set[String] = Set.empty
            ): Seq[(Float, Document)] = {

    val dir = FSDirectory.open(path)
    val reader = DirectoryReader.open(dir)
    try {
      val searcher = new IndexSearcher(reader)
      val hits: TopDocs = searcher.search(query, cutOff)
      val sf = searcher.storedFields()
      val fieldSetOpt =
        if (fields.nonEmpty) Some(fields.asJava) else None

      hits.scoreDocs.toSeq.map { sd =>
        val doc =
          fieldSetOpt match {
            case Some(fset) => sf.document(sd.doc, fset)
            case None       => sf.document(sd.doc)
          }
        sd.score -> doc
      }
    } finally {
      reader.close()
      dir.close()
    }
  }

}
