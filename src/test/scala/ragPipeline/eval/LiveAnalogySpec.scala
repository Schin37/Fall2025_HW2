package ragPipeline.eval

import org.scalatest.funsuite.AnyFunSuite
import ragPipeline.config.AppConfig
import VecOps._
import ragPipeline.PDFsToShardMapReduce.LiveOllamaClient

class LiveAnalogySpec extends AnyFunSuite {

  private val words: Vector[String] = (
    AppConfig.tests.wordSimilarity.flatMap(p => Vector(p.a, p.b)) ++
      AppConfig.tests.wordAnalogy.flatMap(a => Vector(a.a, a.b, a.c, a.expected))
    ).map(_.toLowerCase).distinct.toVector

  private lazy val vectors: Map[String, Array[Float]] = {
    val client = new LiveOllamaClient()
    val vecs = client.embed(words, AppConfig.models.embedModel).map(l2)
    words.zip(vecs).toMap
  }

  private def emb(t: String) = vectors.get(t.toLowerCase)

  private def topK(vec: Array[Float], k: Int, exclude: Set[String] = Set.empty): Vector[(String, Double)] =
    vectors.iterator
      .filterNot { case (tok, _) => exclude.contains(tok) }
      .map { case (tok, v) => tok -> cosine(vec, v) }
      .toVector
      .sortBy(-_._2)
      .take(k)

  test("Analogy accuracy@K on configured triplets") {
    val K      = AppConfig.tests.neighborsTopK.max(1)
    val total  = AppConfig.tests.wordAnalogy.size
    var hits   = 0

    AppConfig.tests.wordAnalogy.foreach { q =>
      val (va, vb, vc) = (emb(q.a).get, emb(q.b).get, emb(q.c).get)
      val target = analogy(va, vb, vc) // a - b + c
      val nn    = topK(target, K, exclude = Set(q.a, q.b, q.c).map(_.toLowerCase))
      val isHit = nn.exists(_._1.equalsIgnoreCase(q.expected))
      if (isHit) hits += 1
      info(s"${q.a}-${q.b}+${q.c} expect=${q.expected}; topK=${nn.map(_._1).mkString(",")}; hit=$isHit")
    }

    val acc = if (total == 0) 0.0 else hits.toDouble / total
    info(f"analogy@${K}: $acc%1.2f ($hits/$total)")
    assert(acc >= 0.10 || total == 0, f"analogy accuracy too low: $acc%1.2f")
  }
}
