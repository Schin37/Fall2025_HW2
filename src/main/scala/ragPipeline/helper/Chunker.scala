package ragPipeline.helper

import ragPipeline.config.AppConfig

/**
 * Chunker
 * -------
 * Splits large text into overlapping chunks with light sanitation
 * so downstream embedding/indexing never sees junk glyphs.
 */
object Chunker {

  // Window / overlap from config
  private val maxCharPerChunk     = AppConfig.chunking.windowChars
  private val overlapCharPerChunk = AppConfig.chunking.overlapChars

  // Hygiene thresholds (can be moved to config later)
  private val MinChunkChars  = 80                 // drop tiny fragments
  private val MinAlphaRatio  = 0.45               // at least ~45% alphabetic
  private val MaxSymbolShare = 0.55               // reject if >55% symbols

  /** Remove replacement chars & controls; normalize whitespace. */
  private def sanitize(s: String): String = {
    s
      .replace('\uFFFD', ' ')
      .replaceAll("\\p{C}", " ")                   // drop control chars
      .replaceAll("[ \\t\\x0B\\f\\r]+", " ")
      .trim
  }

  /** Public normalizer (kept API) — now includes sanitation. */
  def normalize(s: String): String =
    sanitize(s).replaceAll("\\s+", " ").trim

  /** Approx. share of alphabetic chars. */
  private def alphaRatio(s: String): Double =
    if (s.isEmpty) 0.0 else s.count(_.isLetter).toDouble / s.length

  /** Quick symbol share: not letter, digit, or common whitespace/punct. */
  private def symbolShare(s: String): Double = {
    if (s.isEmpty) 1.0
    else {
      val sym = s.count(ch => !(ch.isLetterOrDigit || ch.isWhitespace || isCommonPunct(ch)))
      sym.toDouble / s.length
    }
  }

  private def isCommonPunct(ch: Char): Boolean =
    ".,;:!?\"'()[]{}-–—/\\@&%#+=*<>|".indexOf(ch) >= 0

  /** Conservative junk detector for a candidate piece. */
  private def looksJunk(s: String): Boolean = {
    val t     = sanitize(s)
    val short = t.length < MinChunkChars
    val alpha = alphaRatio(t)
    val sym   = symbolShare(t)
    val pageLabel = t.matches("""^[\W_]*\d{1,4}[\W_]*$""")
    short || alpha < MinAlphaRatio || sym > MaxSymbolShare || pageLabel
  }

  /**
   * Print and return number of chunks (debug).
   * Uses ceil((L - W) / (W - O)) + 1.
   */
  def printNumOfChunks(L: Int, W: Int = maxCharPerChunk, O: Int = overlapCharPerChunk): Int = {
    if (L <= 0) 0
    else if (L <= W) 1
    else {
      val S = math.max(W - O, 1)
      val numChunks = ((L - W + S - 1) / S) + 1 // integer ceil
      // NOTE: comment these out if they’re too chatty
      // println("numChunks"); println(numChunks)
      numChunks
    }
  }

  /**
   * Split into overlapping chunks. Each chunk is at most maxChars long,
   * with 'overlap' overlap. We try to end on '.' or '\n' if it’s not too early.
   * Junky pieces are skipped.
   */
  def split(s: String, maxChars: Int = maxCharPerChunk, overlap: Int = overlapCharPerChunk): Vector[String] = {
    val clean = normalize(s)
    printNumOfChunks(clean.length)
    val out = Vector.newBuilder[String]

    var i = 0
    while (i < clean.length) {
      val end   = (i + maxChars).min(clean.length)
      val slice = clean.substring(i, end)

      // Prefer sentence boundary if it occurs after ~60% of the window
      val boundary = slice.lastIndexWhere(ch => ch == '.' || ch == '\n')
      val piece =
        if (boundary >= (maxChars * 0.60).toInt) slice.substring(0, boundary + 1)
        else slice

      val pieceSan = sanitize(piece)

      if (!looksJunk(pieceSan)) {
        out += pieceSan
      }
      // Advance by chunk length minus overlap (at least 1)
      i += (pieceSan.length - overlap).max(1)
    }

    out.result()
  }

  /** Token helpers below kept as-is, using letter-only heuristic. */
  def isLikelyWord(t: String): Boolean = {
    val s = t.toLowerCase
    s.matches("^[\\p{L}]+$") && s.length >= 2 && s.exists("aeiouy".contains(_))
  }

  def splitWords(text: String): Vector[String] = {
    sanitize(text).toLowerCase
      .replaceAll("[^\\p{L}]+", " ")
      .trim
      .split("\\s+")
      .iterator
      .filter(_.nonEmpty)
      .filter(isLikelyWord)
      .toVector
  }
}
