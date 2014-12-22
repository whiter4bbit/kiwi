package phi.client.example

import java.util.concurrent.CountDownLatch

import com.twitter.util.{Return, Throw}

import phi.client._
import phi.message.EagerMessageSet

object CountProducer {
  case class Options(count: Int = 5, topic: String = "example-topic")

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-count"::count::tail => parse(tail, options.copy(count = count.toInt))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case Nil => options
    case _ => throw new Error("Wrong options")
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val latch = new CountDownLatch(options.count)
    
    def sendAll(producer: QueueProducer, id: Int, i: Int): Unit = {
      producer.send(s"${id}:${i}".getBytes).ensure {
        if (i < 999) sendAll(producer, id, i + 1) else latch.countDown
      } respond {
        case Return(_) => //pass
        case Throw(t) => t.printStackTrace
      }
    }

    (0 until options.count).foreach { id => sendAll(QueueClient("localhost:8080").producer(options.topic), id, 0) }

    latch.await
  }
}

object CountConsumer {
  case class Options(consumers: Int = 5, producers: Int = 5, topic: String = "example-topic")

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-consumers"::count::tail => parse(tail, options.copy(consumers = count.toInt))
    case "-producers"::count::tail => parse(tail, options.copy(producers = count.toInt))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case Nil => options
    case _ => throw new Error
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val latch = new CountDownLatch(options.producers * 1000)
    
    def receive(consumer: QueueGlobalConsumer): Unit = {
      consumer.poll(1).map { set => 
        set.foreach { message => 
          println(s"${new String(message.payload)}")
          latch.countDown
        }
      } ensure {
        receive(consumer)
      } respond {
        case Return(_) => //pass
        case Throw(t) => t.printStackTrace
      }
    }

    (0 until options.consumers).foreach { _ => receive(QueueClient("localhost:8080").consumer(options.topic)) }

    latch.await
  }
}
