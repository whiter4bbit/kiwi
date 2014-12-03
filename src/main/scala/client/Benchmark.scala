package phi.client

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

object BenchmarkProducer {
  def benchmark(url: String, topic: String, count: Int): Unit = {
    val client = QueueClient(url)

    val latch = new CountDownLatch(count)
    val start = System.nanoTime

    val (succeed, failed) = (new AtomicInteger(0), new AtomicInteger(0))

    (0 until count).foreach { _ =>
      client.append(topic, "message-1".getBytes)
        .onSuccess { _ => 
           succeed.incrementAndGet()
           latch.countDown()
        }.onFailure { _ => 
           failed.incrementAndGet() 
           latch.countDown()
        }
    }

    latch.await

    val spent = (System.nanoTime - start)

    println(s"Total time: $spent ns")
    println(s"Average per request: ${spent / count} ns")
    println(s"Succeed: $succeed")
    println(s"Failed: $failed")
  }

  def main(args: Array[String]): Unit = {
    args.toList match {
      case url::topic::count::_ => benchmark(url, topic, count.toInt)
      case _ => System.err.println(s"Usage: ${getClass.getCanonicalName} url topic count")
    }
  }
}

/*
object BenchmarkConsumer {
  def main(args: Array[String]): Unit = {
    args.toList match {
      case url::topic::count::_ => benchmark(url, topic, count.toInt)
      case _ => System.err.println(s"Usage: ${getClass.getCanonicalName} url topic count")
    }
  }
}
*/
