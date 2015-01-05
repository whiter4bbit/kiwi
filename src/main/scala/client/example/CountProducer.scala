package phi.client.example

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CountDownLatch

import com.twitter.util.{Return, Throw}

import phi.client._

object CountProducer {
  case class Options(producers: Int = 5, topic: String = "example-topic", messages: Long = Long.MaxValue, 
    address: String = "localhost:8080")

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-address"::address::tail => parse(tail, options.copy(address = address))
    case "-producers"::producers::tail => parse(tail, options.copy(producers = producers.toInt))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case "-messages"::messages::tail => parse(tail, options.copy(messages = messages.toLong))
    case Nil => options
    case _ => throw new Error("Wrong options")
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val latch = new CountDownLatch(options.producers)
    
    def sendAll(producer: QueueProducer, id: Int, i: Long): Unit = {
      producer.send(s"${id}:${i}".getBytes).ensure {
        if (i < options.messages) sendAll(producer, id, i + 1) else latch.countDown
      } respond {
        case Return(_) => //pass
        case Throw(t) => t.printStackTrace
      }
    }

    (0 until options.producers).foreach { id => sendAll(QueueClient(options.address).producer(options.topic), id, 0) }

    latch.await
  }
}

object CountConsumer {
  case class Options(consumers: Int = 5, messages: Long = Long.MaxValue, topic: String = "example-topic", 
    atLeastOnce: Boolean = false, address: String = "localhost:8080")

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-address"::address::tail => parse(tail, options.copy(address = address))
    case "-consumers"::count::tail => parse(tail, options.copy(consumers = count.toInt))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case "-messages"::messages::tail => parse(tail, options.copy(messages = messages.toLong))
    case "-at-least-once"::atLeastOnce::tail => parse(tail, options.copy(atLeastOnce = atLeastOnce.toBoolean))
    case Nil => options
    case _ => throw new Error
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val count = new AtomicLong(
      if (options.atLeastOnce) options.messages * options.consumers else options.messages      
    )
    val latch = new CountDownLatch(1)
    
    def receive(consumer: GlobalQueueConsumer): Unit = {
      consumer.poll(1).map { set =>
        set.foreach { message => 
          println(s"${new String(message.payload)}")
          if (count.decrementAndGet == 0) latch.countDown
        }
      } ensure {
        receive(consumer)
      } respond {
        case Return(_) => //pass
        case Throw(t) => t.printStackTrace
      }
    }

    def atLeastOnceReceive(consumer: QueueConsumer): Unit = {
      consumer.poll(1) { messages =>
        messages.foreach { message =>
          println(s"${new String(message.payload)}")
          if (count.decrementAndGet == 0) latch.countDown
        }
      } ensure {
        atLeastOnceReceive(consumer)
      } respond {
        case Return(_) => //pass
        case Throw(t) => t.printStackTrace
      }
    }

    (0 until options.consumers).foreach { i => 
      def client = QueueClient(options.address)
      if (options.atLeastOnce) 
        atLeastOnceReceive(client.consumer(options.topic, s"consumer-$i"))
      else
        receive(client.consumer(options.topic))
    }

    latch.await
  }
}
