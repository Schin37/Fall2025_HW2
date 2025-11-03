package ragPipeline.IncrementalDatabaseCreation

import ragPipeline.config.AppConfig
import ragPipeline.models.Ollama


import scala.collection.mutable.ArrayBuffer

object Embedder {
  /** Returns float vectors for the input texts; uses cache and batching. */
  def embedAll(
                texts: Vector[String],
                batchSize: Int = AppConfig.embed.batchSize,
                keepAlive: String = AppConfig.embed.keepAlive,
                model: String = AppConfig.models.embedModel
              ): Vector[Array[Float]] = {

    // Single client, warm once
    val client = new Ollama()
    client.embed(Vector("warmup"), model = model, keepAlive = Some(keepAlive))

    val n = texts.length

    // Mutable output so we can assign by index
    val out: ArrayBuffer[Array[Float]] = ArrayBuffer.fill[Array[Float]](n)(null)

    // Track which indices need embeddings (cache misses) and their texts
    val needIdx  = ArrayBuffer.empty[Int]
    val toEmbed  = ArrayBuffer.empty[String]

    var i = 0
    while (i < n) {
      val t = texts(i)
      EmbCache.get(t) match {
        case Some(vec) =>
          out(i) = vec
        case None =>
          needIdx += i
          toEmbed += t
      }
      i += 1
    }

    // Batch the misses
    var cursor = 0
    toEmbed.grouped(batchSize).foreach { group =>
      // Ensure this returns Vector[Array[Float]]
      val vecs: Vector[Array[Float]] =
        client.embed(group.toVector, model = model, keepAlive = Some(keepAlive))

      // Map batch results back to original positions
      val idxs = needIdx.slice(cursor, cursor + group.size)
      var j = 0
      while (j < vecs.length) {
        val textIndex = idxs(j)
        val vec       = vecs(j)
        // Cache & assign
        EmbCache.put(texts(textIndex), vec)
        out(textIndex) = vec
        j += 1
      }
      cursor += group.size
    }

    // Fill any leftover nulls (shouldn’t happen, but be defensive)
    val dim = out.find(_ != null).map(_.length).getOrElse(0)
    var k = 0
    while (k < n) {
      if (out(k) == null)
        out(k) = new Array[Float](dim) // zeros
      k += 1
    }

    out.toVector
  }
}
