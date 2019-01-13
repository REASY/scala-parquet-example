trait BenchmarkHelper {
  def maxTries: Int = 3

  def bench(maxTries: Int, name: String, f: => Unit): Unit = {
    assert (maxTries != 0)
    var i: Int = 0
    val s = System.currentTimeMillis()
    while (i < maxTries) {
      f
      i += 1
    }
    val e = System.currentTimeMillis()
    val d = e - s
    val avg = d.toDouble / maxTries
    println(s"$name executed $maxTries. AVG time: $avg ms")
  }

  def meter[T](what: String, body: => T, logger: String => Unit = println): T = {
    val s = System.currentTimeMillis()
    val result = body
    val e = System.currentTimeMillis()
    val d = e - s
    logger(s"'$what' executed in ${d} ms")
    result
  }
}
