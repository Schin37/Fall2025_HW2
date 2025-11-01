package ragPipeline.eval

import org.scalatest.funsuite.AnyFunSuite
import ragPipeline.config.AppConfig
import VecOps._
import ragPipeline.PDFsToShardMapReduce.LiveOllamaClient

class LiveSimilaritySpec extends AnyFunSuite {

  // Build list of words to embed live (from config)
  private val words: Vector[String] = (
    AppConfig.tests.wordSimilarity.flatMap(p => Vector(p.a, p.b)) ++
      AppConfig.tests.wordAnalogy.flatMap(a => Vector(a.a, a.b, a.c, a.expected))
    ).map(_.toLowerCase).distinct.toVector

  // Embed ONCE for the suite
  private lazy val vectors: Map[String, Array[Float]] = {
    val client = new LiveOllamaClient()
    val vecs = client.embed(words, AppConfig.models.embedModel).map(l2)
    words.zip(vecs).toMap
  }

  private def emb(t: String) = vectors.get(t.toLowerCase)

  test("Live embeddings available for all configured words") {
    val missing = words.filterNot(vectors.contains)
    assert(missing.isEmpty, s"Missing embeddings for: $missing")
  }

  test("Cosine similarity for configured pairs is reasonable (>= min)") {
    val min = 0.20  // tune later if needed
    AppConfig.tests.wordSimilarity.foreach { p =>
      val (va, vb) = (emb(p.a).get, emb(p.b).get)
      val s = cosine(va, vb)
      assert(!s.isNaN, s"NaN cosine for ${p.a} ~ ${p.b}")
      assert(s >= min, f"${p.a}~${p.b} too low: $s%1.3f < $min%1.3f")
    }
  }
}
