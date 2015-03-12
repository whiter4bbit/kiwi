package phi

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest._

import phi.message.{Message, MessageBinaryFormat}

import phi.bytes._
import phi.io._
import PhiFiles._

class KiwiSpec extends FlatSpec with Matchers {
  implicit val format = MessageBinaryFormat(10 * 1024)

  def messageBatch(payload: Array[Byte]) = {
    if (payload.length > 10 * 1024) throw new Error("wrong size!!!!")
    format.write(Message(payload)::Nil, ByteChunk.builder).get
  }

  trait MessageBatchView {
    def count: Int
    def iterator: Iterator[Message]
  }

  implicit def byteChunk2MessageBatchView(batch: ByteChunkAndOffset)(implicit format: MessageBinaryFormat)  = {
    val iterator = new BinaryFormatIterator(batch.chunk, format)
    val messages = iterator.toList
    new MessageBatchView {
      def count = messages.size
      def iterator = messages.iterator
    }
  }

  it should "guarantee at most once delivery for global consumers" in {
    withTempDir("persistent-queue-spec") { dir =>
      val queue = Kiwi.start(Config().copy(baseDir = dir))

      val producer = queue.getProducer("ordered-topic")

      val count = 10000

      (0 until count).foreach { seq =>
        producer.append(messageBatch(seq.toString.getBytes))
      }

      val received = Array.fill(count)(new AtomicInteger)

      @volatile var running = true

      val latch = new CountDownLatch(count)

      val consumer = new Runnable() {
        def run(): Unit = {
          while (running) {
            val batch = queue.getConsumer("ordered-topic").next(5)
            batch.iterator.foreach { message =>
              latch.countDown
              received(new String(message.payload).toInt).incrementAndGet()
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
