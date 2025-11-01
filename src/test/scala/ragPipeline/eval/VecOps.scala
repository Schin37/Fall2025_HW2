package ragPipeline.eval

object VecOps {
  def dot(a: Array[Float], b: Array[Float]): Double = {
    var s = 0.0; var i = 0; val n = Math.min(a.length, b.length)
    while (i < n) { s += a(i) * b(i); i += 1 }
    s
  }
  def norm(a: Array[Float]): Double = Math.sqrt(dot(a, a))
  def l2(a: Array[Float]): Array[Float] = {
    val n = norm(a); if (n == 0.0) a.clone() else {
      val out = new Array[Float](a.length)
      var i = 0; while (i < a.length) { out(i) = (a(i)/n).toFloat; i += 1 }
      out
    }
  }
  def cosine(a: Array[Float], b: Array[Float]): Double = {
    val na = norm(a); val nb = norm(b)
    if (na == 0.0 || nb == 0.0) 0.0 else dot(a,b)/(na*nb)
  }
  def analogy(a: Array[Float], b: Array[Float], c: Array[Float]): Array[Float] = {
    val n = Math.min(a.length, Math.min(b.length, c.length))
    val out = new Array[Float](n)
    var i = 0; while (i < n) { out(i) = (a(i) - b(i) + c(i)).toFloat; i += 1 }
    out
  }
}
