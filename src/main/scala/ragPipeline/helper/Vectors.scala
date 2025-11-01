package ragPipeline.helper

/**
 * Vectors
 * -------
 * Provides vector utility functions used for normalization
 * and similarity computations in the RAG pipeline.
 */
object Vectors {

  /**
   * Performs L2 normalization on an embedding vector.
   * Divides each element by the square root of the sum of squares.
   * Returns the same vector if it is all zeros (to avoid division by zero).
   */
  def l2Normalization(embeddings: Array[Float]): Array[Float] = {
    val l2Norm = math.sqrt(embeddings.map(x => x * x).sum.toDouble)
    if (l2Norm == 0.0) {
      embeddings
    } else {
      embeddings.map(x => (x / l2Norm).toFloat)
    }
  }

}
