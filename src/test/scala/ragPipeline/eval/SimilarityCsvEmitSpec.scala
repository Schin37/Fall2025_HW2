package ragPipeline.eval

import org.scalatest.funsuite.AnyFunSuite
import ragPipeline.PDFsToShardMapReduce.LiveOllamaClient
import ragPipeline.config.AppConfig

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

/** Emits a CSV of pairwise metrics for the configured wordSimilarity pairs:
 * columns: a,b,dot,cosine,norm_a,norm_b
 *
 * Notes:
 * - This spec embeds WITHOUT L2 normalization so "dot" is meaningful.
 * - Cosine is then computed from raw vectors: dot/(||a||*||b||).
 * - Output file: ${app.paths.outDir}/similarity_metrics.csv
 */
class SimilarityCsvEmitSpec extends AnyFunSuite {

  private def dot(a: Array[Float], b: Array[Float]): Double = {
    val n = math.min(a.length, b.length)
    var s = 0.0; var i = 0
    while (i < n) { s += a(i) * b(i); i += 1 }
    s
  }

  private def norm(a: Array[Float]): Double = math.sqrt(dot(a, a))

  // All words we need to embed (pairs + analogy vocab for consistency cache)
  private val words: Vector[String] =
    (AppConfig.tests.wordSimilarity.flatMap(p => Vector(p.a, p.b)) ++
      AppConfig.tests.wordAnalogy.flatMap(a => Vector(a.a, a.b, a.c, a.expected)))
      .map(_.toLowerCase).distinct.toVector

  // Embed ONCE for the suite (RAW vectors: no l2 here)
  private lazy val vectors: Map[String, Array[Float]] = {
    val client = new LiveOllamaClient()
    val vecs   = client.embed(words, AppConfig.models.embedModel)
    words.zip(vecs).toMap
  }

  private def emb(t: String) = vectors.get(t.toLowerCase)

  // helper replacement for scala.util.Using
  private def using[A <: AutoCloseable, B](res: A)(f: A => B): B =
    try f(res) finally if (res != null) res.close()

  test("Emit CSV with dot & cosine for configured pairs") {
    val outDir  = AppConfig.paths.outDir.toString
    Files.createDirectories(Paths.get(outDir))
    val outPath = Paths.get(outDir, "similarity_metrics.csv")

    using(Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) { w =>
      w.write("a,b,dot,cosine,norm_a,norm_b\n")

      AppConfig.tests.wordSimilarity.foreach { p =>
        val va  = emb(p.a).getOrElse(sys.error(s"Missing embedding for ${p.a}"))
        val vb  = emb(p.b).getOrElse(sys.error(s"Missing embedding for ${p.b}"))
        val d   = dot(va, vb)
        val na  = norm(va)
        val nb  = norm(vb)
        val cos = if (na == 0.0 || nb == 0.0) 0.0 else d / (na * nb)

        def csvEscape(s: String) = "\"" + s.replace("\"", "\"\"") + "\""
        w.write(s"${csvEscape(p.a)},${csvEscape(p.b)},$d,$cos,$na,$nb\n")
      }
    }

    info(s"Wrote CSV: $outPath")
    assert(Files.exists(outPath), s"CSV not found at $outPath")
  }
}
