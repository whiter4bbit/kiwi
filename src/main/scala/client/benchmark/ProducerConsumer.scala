package phi.client.benchmark

import java.util.concurrent.CountDownLatch

import phi.WallClock
import phi.client._
import phi.message._

object ProducerConsumer {
  case class Options(messages: Int = 10000, size: Int = 1024, producerBatch: Int = 10, consumerBatch: Int = 10, consumers: Int = 10,
    address: String = "localhost:8080", topic: String = "example-topic", consumerId: String = "consumer-1", offsetConsumer: Boolean = false)

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-messages"::messages::tail => parse(tail, options.copy(messages = messages.toInt))
    case "-size"::size::tail => parse(tail, options.copy(size = size.toInt))
    case "-producer-batch"::producerBatch::tail => parse(tail, options.copy(producerBatch = producerBatch.toInt))
    case "-consumer-batch"::consumerBatch::tail => parse(tail, options.copy(consumerBatch = consumerBatch.toInt))
    case "-address"::address::tail => parse(tail, options.copy(address = address))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case "-consumer-id"::consumerId::tail => parse(tail, options.copy(consumerId = consumerId))
    case "-offset-consumer"::tail => parse(tail, options.copy(offsetConsumer = true))
    case "-consumers"::consumers::tail => parse(tail, options.copy(consumers = consumers.toInt))
    case Nil => options
    case _ => throw new Error
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val client = QueueClient(options.address)
    val producer = client.producer(options.topic)

    def produce(n: Int): Unit = {
      val now = WallClock.millis
      val messageSet = EagerMessageSet((0 until options.producerBatch).map { _ =>
        Message(now.toString.getBytes)
      }.toList)

      producer.send(messageSet).ensure {
        if (n > 0) produce(n - options.producerBatch)
      }
    }

    @volatile var lagSum: Long = 0
    @volatile var minLag: Long = 0
    @volatile var maxLag: Long = 0

    val offsetConsumer = client.consumer(options.topic, options.consumerId)
    val globalConsumer = client.consumer(options.topic)

    val latch = new CountDownLatch(options.messages)

    def processMessages(messages: List[MessageAndOffset]): Unit = {
      val now = WallClock.millis
      messages.foreach { message =>
        val lag = now - new String(message.payload).toLong
        lagSum += lag
        minLag = Math.min(minLag, lag)
        maxLag = Math.max(maxLag, lag)
        latch.countDown
      }
    }

    def consumeOffset(): Unit = {
      offsetConsumer.poll(options.consumerBatch)(processMessages) ensure { consumeOffset() }
    }

    def consumeGlobal(): Unit = {
      globalConsumer.poll(options.consumerBatch).map(processMessages) ensure { consumeGlobal() }
    }

    produce(options.messages)

    val now = WallClock.millis

    (0 until options.consumers).foreach { _ =>
      if (options.offsetConsumer) {
        consumeOffset()
      } else {
        consumeGlobal()
      }
    }

    latch.await

    printf("lag:\n")
    printf("avg = %2.2f\n", (lagSum.toDouble / options.messages))
    printf("min = %d\n", minLag)
    printf("max = %d\n", maxLag)

    printf("consumer:\n")

    val total = WallClock.millis - now
    printf("total time = %d ms\n", total)
    printf("avg per batch = %2.2f ms\n", (total.toDouble / (options.messages / options.consumerBatch)))
  }
}
