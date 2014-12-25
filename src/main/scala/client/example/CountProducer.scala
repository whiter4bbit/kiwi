package phi.client.example

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CountDownLatch

import com.twitter.util.{Return, Throw}

import phi.client._
import phi.message.EagerMessageSet

object CountProducer {
  case class Options(producers: Int = 5, topic: String = "example-topic", messages: Long = Long.MaxValue)

  def parse(args: List[String], options: Options = Options()): Options = args match {
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

    (0 until options.producers).foreach { id => sendAll(QueueClient("localhost:8080").producer(options.topic), id, 0) }

    latch.await
  }
}

object CountConsumer {
  case class Options(consumers: Int = 5, producers: Int = 5, messages: Long = Long.MaxValue, topic: String = "example-topic", tx: Boolean = false)

  def parse(args: List[String], options: Options = Options()): Options = args match {
    case "-consumers"::count::tail => parse(tail, options.copy(consumers = count.toInt))
    case "-producers"::count::tail => parse(tail, options.copy(producers = count.toInt))
    case "-topic"::topic::tail => parse(tail, options.copy(topic = topic))
    case "-messages"::messages::tail => parse(tail, options.copy(messages = messages.toLong))
    case "-tx"::tx::tail => parse(tail, options.copy(tx = tx.toBoolean))
    case Nil => options
    case _ => throw new Error
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList)

    val count = new AtomicLong(options.messages)
    val latch = new CountDownLatch(1)
    
    def receive(consumer: QueueGlobalConsumer): Unit = {
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

    def txReceive(consumer: QueueConsumer): Unit = {
      consumer.poll(1).map { messages =>
        messages.foreach { message =>
          println(s"${new String(message.payload)}")
          if (count.decrementAndGet == 0) latch.countDown
        }
        messages.lastOption.map { messageAndOffset =>
          consumer.commit(messageAndOffset.offset)
        }
      } ensure {
        txReceive(consumer)
      } respond {
        case Return(_) => //pass
        case Throw(t) => t.printStackTrace
      }
    }

    (0 until options.consumers).foreach { i => 
      def client = QueueClient("localhost:8080")
      if (options.tx) 
        txReceive(client.consumer(options.topic, s"consumer-$i"))
      else
        receive(client.consumer(options.topic))
    }

    latch.await
  }
}
