package phi

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import phi.message._

import phi.io._
import PhiFiles._

import org.scalatest._

class AwaitableConsumerSpec extends FlatSpec with Matchers {
  def messageBatch(payload: Array[Byte]) = 
    SimpleMessageBatch(List(Message(payload)))

  "AwaitableConsumer" should "return batch when messages available" in {
    withTempDir("awaitable-consumer") { dir =>
      val kiwi = Kiwi.start(dir)
      val consumer = AwaitableConsumer.start(kiwi)

      val ConsumersCount = 10000
      val latch = new CountDownLatch(ConsumersCount)

      (0 until ConsumersCount).foreach { i =>
        val topic = i % 2
        val batch = consumer.await(s"example-topic-$topic", s"consumer-$i", 1, (5 seconds))
        batch.map { b => if (b.count > 0) latch.countDown }
      }

      kiwi.getProducer("example-topic-0").append(messageBatch("message-1".getBytes))
      kiwi.getProducer("example-topic-1").append(messageBatch("message-2".getBytes))

      latch.await(6, TimeUnit.SECONDS) should be (true)
    }
  }
}
