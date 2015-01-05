package phi

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest._

import phi.message.Message
import phi.io._
import PhiFiles._

class PersistentQueueSpec extends FlatSpec with Matchers {
  "PersistentQueue" should "guarantee at most once delivery for global consumers" in {
    withTempDir("persistent-queue-spec") { dir =>
      val queue = new PersistentQueue(dir)

      val producer = queue.getProducer("ordered-topic")

      val count = 10000

      (0 until count).foreach { seq =>
        producer.append(seq.toString.getBytes)
      }

      val received = Array.fill(count)(new AtomicInteger)

      @volatile var running = true

      val latch = new CountDownLatch(count)

      val consumer = new Runnable() {
        def run(): Unit = {
          while (running) {
            val pointer = queue.getConsumer("ordered-topic").next(5)
            Message.fromPointer(pointer).map { messages =>
              messages.foreach { message =>
                latch.countDown
                received(new String(message.payload).toInt).incrementAndGet()
              }
            }
          }
        }
      }

      val threads = (0 until 5).map { _ =>
        val thread = new Thread(consumer)
        thread.start
        thread
      }

      latch.await

      Thread.sleep(3000)

      running = false

      threads.foreach(_.join)

      received.map(_.get).deep should be (Array.fill(count)(1).deep)
    }
  }
}
