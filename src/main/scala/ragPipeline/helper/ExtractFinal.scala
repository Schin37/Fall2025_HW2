package ragPipeline.helper

import cats.implicits.catsSyntaxApplicativeError
import io.circe.HCursor
import io.circe.parser.parse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path => HPath}
import ragPipeline.config.AppConfig

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
 * ExtractFinal
 * ------------
 * Purpose: Read MapReduce reducer output (a directory with part-* files) and
 * return one consolidated context string suitable for passing to an LLM.
 */
object ExtractFinal {

  // === Filtering & diversification helpers (drop-in) ===
  private case class Hit(score: Double, text: String, source: String, chunkId: Int)

  private def looksReferences(t: String): Boolean = {
    val s = t.toLowerCase.trim
    val header = s.startsWith("references") || s.startsWith("acknowledg") || s.contains("in proceedings") ||
      s.contains("acm press") ||
      s.contains("conference on")
    val citeBrackets = "\\[[0-9]{1,3}\\]".r.findAllIn(s).length
    val years = "(19|20)\\d{2}".r.findAllIn(s).length
    val commas = s.count(_ == ',')
    header || (citeBrackets >= 3 && years >= 2 && commas >= 6)
  }

  private def sanitize(s: String): String =
    s.replace('\uFFFD',' ')
      .replaceAll("\\p{C}", " ")
      .replaceAll("[ \\t\\x0B\\f\\r]+"," ")
      .trim

  private def boostSection(h: Hit): Hit = {
    val s = h.text.toLowerCase
    val b =
      (if (s.startsWith("abstract")) 0.08 else 0.0) +
        (if (s.startsWith("introduction") || s.startsWith("1. introduction")) 0.05 else 0.0) +
        (if (s.startsWith("conclusion")   || s.startsWith("conclusions"))    0.03 else 0.0)
    h.copy(score = h.score + b)
  }

  private def capPerDocPage(hits: Seq[Hit], perDoc: Int = 2, perPage: Int = 1): Seq[Hit] = {
    val seenPerDoc  = scala.collection.mutable.Map.empty[String, Int]
    val seenPerPage = scala.collection.mutable.Map.empty[(String, Int), Int]
    hits.filter { h =>
      val okDoc  = seenPerDoc.getOrElse(h.source, 0) < perDoc
      val pageId = (h.source, h.chunkId / 1000) // adjust if your chunk→page mapping differs
      val okPage = seenPerPage.getOrElse(pageId, 0) < perPage
      val keep   = okDoc && okPage
      if (keep) {
        seenPerDoc.update(h.source,  seenPerDoc.getOrElse(h.source, 0) + 1)
        seenPerPage.update(pageId,   seenPerPage.getOrElse(pageId, 0) + 1)
      }
      keep
    }
  }

  private def textSim(a: String, b: String): Double = {
    // ultra-cheap Jaccard on lowercase word sets; replace with cosine over embeds if available
    val A = a.toLowerCase.split("\\W+").toSet
    val B = b.toLowerCase.split("\\W+").toSet
    if (A.isEmpty || B.isEmpty) 0.0 else (A.intersect(B).size.toDouble / A.union(B).size)
  }

  private def mmr(cands: Vector[Hit], k: Int, lambda: Double = 0.7): Vector[Hit] = {
    var selected = Vector.empty[Hit]
    var rest     = cands
    while (selected.size < k && rest.nonEmpty) {
      val pick = rest.maxBy { h =>
        val rel = h.score
        val red = if (selected.isEmpty) 0.0 else selected.map(s => textSim(h.text, s.text)).max
        lambda * rel - (1 - lambda) * red
      }
      selected :+= pick
      rest = rest.filterNot(x => (x.source == pick.source) && (x.chunkId == pick.chunkId))
    }
    selected
  }

  private def packByBudget(hits: Seq[Hit], maxChars: Int): Seq[Hit] = {
    val kept = collection.mutable.ArrayBuffer.empty[Hit]
    var used = 0
    hits.foreach { h =>
      val add = h.text.length + 2
      if (used + add <= maxChars) { kept += h; used += add }
    }
    kept.toSeq
  }

  def readContextFromDir(
                          outDir: String,
                          conf: Configuration,
                          maxChars: Int = AppConfig.chunking.maxContextBlock
                        ): String = {

    val fs = new HPath(outDir).getFileSystem(conf)
    val dir = new HPath(outDir)

    // find all reducer output files (part-r-*)
    val parts = fs.listStatus(dir)
      .map(_.getPath)
      .filter(p => p.getName.startsWith(AppConfig.paths.eachPartFileNamePrefix))

    // helper to read lines
    def linesOf(p: HPath): Iterator[String] = {
      val in = fs.open(p)
      val br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
      new Iterator[String] {
        private var nextLine: String = br.readLine()
        override def hasNext: Boolean = nextLine != null
        override def next(): String = {
          val res = nextLine
          nextLine = br.readLine()
          if (nextLine == null) { br.close(); in.close() }
          res
        }
      }
    }

    val allLines = parts.iterator.flatMap(linesOf).map(_.trim).toVector

    val contextLine = allLines.find(_.contains("\"context\""))
    contextLine match {
      case Some(line) =>
        val ctx = extractContextFromJsonLine(line)
          .orElse(extractContextByRegex(line))
          .getOrElse(line)
        return clip(normalize(ctx), maxChars)

      case None =>
        val hitsLine = allLines.find(_.contains("\"hits\""))
        hitsLine match {
          case Some(hline) =>
            val blocks = extractBlocksFromHitsJson(hline)
              .orElse(extractBlocksFromHitsRegex(hline))
              .getOrElse(Nil)

            val deduped = dedupe(blocks.map(normalize)).filter(_.length >= 40)
            val filtered = deduped.filterNot(looksReferences)
            val joined = filtered.mkString("\n\n")
            clip(joined, maxChars)

          case None =>
            val joined = allLines.filter(_.nonEmpty).mkString("\n")
            clip(normalize(joined), maxChars)
        }
    }
  }

  // ------------------- helpers -------------------

  private def normalize(s: String): String =
    s.replaceAll("\\s+", " ").replace(" ? ", "? ").trim

  private def clip(s: String, max: Int): String =
    if (s.length <= max) s else s.take(max) + "…"

  private def dedupe(ss: Seq[String]): Seq[String] = {
    val seen = scala.collection.mutable.HashSet[String]()
    ss.filter { s =>
      val k = s.take(160)
      if (seen(k)) false else { seen += k; true }
    }
  }

  private def extractContextFromJsonLine(line: String): Option[String] =
    parse(line).toOption.flatMap(js => js.hcursor.get[String]("context").toOption)

  private def extractBlocksFromHitsJson(line: String): Option[Seq[String]] =
    parse(line).toOption.flatMap { js =>
      val c: HCursor = js.hcursor
      c.downField("hits").focus.flatMap(_.asArray).map { arr =>
        val items = arr.flatMap { j =>
          val hc = j.hcursor
          val score = hc.get[Double]("score").getOrElse(0.0)
          val src = hc.get[String]("source").getOrElse("")
          val chunk = hc.get[Int]("chunk_id").getOrElse(-1)
          val txt = hc.get[String]("text").orElse(hc.get[String]("preview")).getOrElse("")
          Option(txt).filter(_.nonEmpty).map(t => (score, src, chunk, t))
        }
        items.sortBy(-_._1).map {
          case (s, src, ch, t) =>
            val head =
              if (src.nonEmpty) f"[$src #$ch | score=$s%.4f]"
              else f"[#${ch} | score=$s%.4f]"
            s"$head\n$t"
        }.toSeq
      }
    }

  private def extractContextByRegex(line: String): Option[String] = {
    val r = """.*"context"\s*:\s*"(.*)".*""".r
    line match {
      case r(body) => Some(unescape(body))
      case _       => None
    }
  }

  private def extractBlocksFromHitsRegex(line: String): Option[Seq[String]] = {
    val textR = """"text"\s*:\s*"(.*?)"""".r
    val previewR = """"preview"\s*:\s*"(.*?)"""".r
    val scoreR = """"score"\s*:\s*([0-9.]+)""".r
    val srcR = """"source"\s*:\s*"(.*?)"""".r
    val chunkR = """"chunk_id"\s*:\s*([0-9]+)""".r

    def all(r: scala.util.matching.Regex): Seq[String] =
      r.findAllMatchIn(line).map(_.group(1)).toSeq

    val texts = all(textR) match {
      case Nil => all(previewR)
      case xs  => xs
    }
    val scores = all(scoreR).map(_.toDouble)
    val sources = all(srcR)
    val chunks = all(chunkR).map(_.toInt)

    if (texts.isEmpty) None
    else {
      val tuples = texts.indices.map { i =>
        val sc = if (i < scores.size) scores(i) else 0.0
        val src = if (i < sources.size) sources(i) else ""
        val ch = if (i < chunks.size) chunks(i) else -1
        (sc, src, ch, unescape(texts(i)))
      }
      val blocks = tuples.sortBy(-_._1).map {
        case (s, src, ch, t) =>
          val head =
            if (src.nonEmpty) f"[$src #$ch | score=$s%.4f]"
            else f"[#${ch} | score=$s%.4f]"
          s"$head\n$t"
      }
      Some(blocks)
    }
  }

  private def unescape(s: String): String =
    s.replace("\\r", "\r")
      .replace("\\n", "\n")
      .replace("\\t", "\t")
      .replace("\\\"", "\"")
      .replace("\\\\", "\\")
}
