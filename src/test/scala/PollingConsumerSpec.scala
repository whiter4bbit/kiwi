package phi

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import phi.message._
import phi.io._
import PhiFiles._

import org.scalatest._

class PollingConsumerSpec extends FlatSpec with Matchers {
  "PollingConsumer" should "return message pointer when messages available" in {
    withTempDir("polling-consumer") { dir =>
      val queue = new PersistentQueue(dir)
      val consumer = PollingConsumer.start(queue)

      val ConsumersCount = 10000
      val latch = new CountDownLatch(ConsumersCount)

      (0 until ConsumersCount).foreach { i =>
        val topic = i % 2
        val broker = consumer.request(s"example-topic-$topic", Some(s"consumer-$i"), 1, (5 seconds))
        broker.recv.sync().map { p => if (p.count > 0) latch.countDown }
      }

      queue.getProducer("example-topic-0").append("message-1".getBytes)
      queue.getProducer("example-topic-1").append("message-2".getBytes)

      latch.await(6, TimeUnit.SECONDS) should be (true)
    }
  }
}
