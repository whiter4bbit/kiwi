package phi.client.benchmark

import java.util.concurrent.CountDownLatch
import com.twitter.util.Future

import phi.client._
import phi.message._

object ProducerBenchmark {
  case class Options(producers: Int = 1, messages: Int = 1000000, size: Int = 1024, batch: Int = 10, 
      address: String = "localhost:5432", topic: String = "example-topic")

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-producers"::producers::tail => parse(tail, options.copy(producers = producers.toInt))
    case "-messages"::messages::tail => parse(tail, options.copy(messages = messages.toInt))
    case "-size"::size::tail => parse(tail, options.copy(size = size.toInt))
    case "-batch"::batch::tail => parse(tail, options.copy(batch = batch.toInt))
    case "-address"::address::tail => parse(tail, options.copy(address = address))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case Nil => options
    case _ => throw new Error
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val latch = new CountDownLatch(options.producers * (options.messages / options.batch))

    val message = Array.ofDim[Byte](options.size)

    val messages = (0 until options.batch).map(_ => Message(message)).toList

    def produce(n: Int, producer: QueueProducer): Unit = {
       producer.send(messages).map { _ =>
         latch.countDown
       } ensure {
         if (n > 0) produce(n - 1, producer)
       }
    }

    val client = QueueClient(options.address)

    val start = System.currentTimeMillis
    (0 until options.producers).foreach { i =>
      produce(options.messages / options.batch, client.producer(options.topic))
    }
      
    latch.await
    
    val spent = System.currentTimeMillis - start
    val avg = (options.producers * options.messages) / (spent.toDouble / 1000)

    printf("Avg Throughput = %.2f messages/sec\n", avg)
    printf("Avg Latency = %.2f ms\n", (spent / (options.producers * (options.messages / options.batch)).toDouble))
  }
}

object ConsumerBenchmark {
  case class Options(consumers: Int = 1, batch: Int = 10, max: Int = 1000000, ack: Boolean = false, 
      poll: Boolean = false, topic: String = "example-topic", address: String = "localhost:5432")

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-consumers"::consumers::tail => parse(tail, options.copy(consumers = consumers.toInt))
    case "-batch"::batch::tail => parse(tail, options.copy(batch = batch.toInt))
    case "-max"::max::tail => parse(tail, options.copy(max = max.toInt))
    case "-ack"::tail => parse(tail, options.copy(ack = true))
    case "-poll"::tail => parse(tail, options.copy(poll = true))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case "-address"::address::tail => parse(tail, options.copy(address = address))
    case Nil => options
    case _ => throw new Error
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val latch = new CountDownLatch(options.max)

    def consume(f: (List[Message] => Unit) => Future[Unit]): Unit = {
      f { messages =>
        messages.foreach(_ => latch.countDown)
      } ensure {
        consume(f)
      }
    }

    val start = System.currentTimeMillis

    val client = QueueClient(options.address)

    (0 until options.consumers).foreach { i =>
      val f: ((List[Message] => Unit) => Future[Unit]) = (options.ack, options.poll) match {
        case (false, false) => {
          val consumer = client.consumer(options.topic)
          (cb) => consumer.fetch(options.batch).map(cb)
        }
        case (false, true) => {
          val consumer = client.consumer(options.topic)
          (cb) => consumer.poll(options.batch).map(cb)
        }
        case (true, false) => {
          val consumer = client.consumer(options.topic, s"consumer-$i")
          (cb) => consumer.fetch(options.batch)(cb)
        }
        case (true, true) => {
          val consumer = client.consumer(options.topic, s"consumer$i")
          (cb) => consumer.poll(options.batch)(cb)
        }
      }

      consume(f)
    }

    latch.await

    val spent = (System.currentTimeMillis - start) / 1000
    val avg = options.max / spent.toDouble

    printf(s"Avg Throughput = %.2f messages/sec", avg)

  }
}
