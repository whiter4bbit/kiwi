package phi

import com.twitter.concurrent.{Broker, Offer}

import scala.concurrent.duration._

import phi.message.FileChannelMessagesPointer

class PollingConsumer private (queue: PersistentQueue) {
  case class Request(topic: String, consumer: Option[String], count: Int, broker: Broker[FileChannelMessagesPointer], deadline: Deadline)

  private var requests = List.empty[Request]

  private class PollThread extends Thread {
    override def run(): Unit = tick()
  }

  private def start(): Unit = {
    new PollThread().start()
  }

  private def process(requests: List[Request], pending: List[Request]): List[Request] = requests match {
    case Request(_, _, 0, _, _)::tail => {
      process(tail, pending)
    }
    case Request(topic, consumer, count, broker, deadline)::tail if deadline.isOverdue => {
      val queueConsumer = consumer
          .map(queue.getConsumer(topic, _))
          .getOrElse(queue.getConsumer(topic))
      broker.send(queueConsumer.next(count)).sync()
      process(tail, pending)
    }
    case (r @ Request(topic, consumer, count, broker, _))::tail => {
      val queueConsumer = consumer
          .map(queue.getConsumer(topic, _))
          .getOrElse(queue.getConsumer(topic))
      val pointer = queueConsumer.next(count)
      if (pointer.count > 0) {
        broker.send(pointer).sync()
        process(tail, pending)
      } else {
        process(tail, pending :+ r)
      }
    }
    case Nil => pending
  }

  private def tick(): Unit = {
    this.synchronized {
      requests = process(requests, List.empty)
    }

    Thread.sleep(100)
    tick()
  }

  def request(topic: String, consumer: Option[String], count: Int, timeout: FiniteDuration): Broker[FileChannelMessagesPointer] = this.synchronized {
    val broker = new Broker[FileChannelMessagesPointer]
    requests = requests :+ Request(topic, consumer, count, broker, Deadline.now + timeout)
    broker
  }

  def request(topic: String, count: Int, timeout: FiniteDuration): Broker[FileChannelMessagesPointer] = 
    request(topic, None, count, timeout)
}

object PollingConsumer {
  def start(queue: PersistentQueue): PollingConsumer = {
    val consumer = new PollingConsumer(queue)
    consumer.start()
    consumer
  }
}
