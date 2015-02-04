package phi.benchmark

import java.nio.file.{Files => JFiles}
import phi._
import phi.message._
import phi.io._

object LogBenchmark {
  def measure(times: Int)(f: => Unit): Long = {
    val start = System.currentTimeMillis
    (0 until times).foreach(_ => f)
    System.currentTimeMillis - start
  }

  def main(args: Array[String]): Unit = {
    val dir = JFiles.createTempDirectory(PhiFiles.tempDirectory, "log-benchmark")
    try {
      val log = Log.open(dir, "log-benchmark")
      val batch = SimpleMessageBatch(
        (0 until 1000).map(_ => Message("a".getBytes)).toList
      )
      (0 until 10).foreach(_ => log.append(batch))

      val warmupDuration = measure(10000)(log.read(0, 1000))
      val duration = measure(10000)(log.read(0, 1000))

      println(s"warmup - ${warmupDuration}ms")
      println(s"duration - ${duration}ms")
    } finally {
      dir.deleteDirectory
    }
  }
}
