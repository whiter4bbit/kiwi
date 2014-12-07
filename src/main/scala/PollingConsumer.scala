package phi

import com.twitter.concurrent.{Broker, Offer}

import scala.concurrent.duration._

class PollingConsumer(queue: PersistentQueue) {
  case class Request(topic: String, consumer: Option[String], count: Int, broker: Broker[List[Message]], timeout: Duration, ts: Long)

  private var requests = List.empty[Request]

  private class PollThread extends Thread {
    override def run(): Unit = tick()
  }

  new PollThread().start()

  private def process(requests: List[Request], pending: List[Request]): List[Request] = requests match {
    case Request(_, _, count, _, timeout, ts)::tail if (count == 0 || timeout.toMillis + ts < System.currentTimeMillis) => 
      process(tail, pending)
    case (r @ Request(topic, consumer, count, broker, timeout, ts))::tail => {
      val queueConsumer = consumer
          .map(queue.getConsumer(topic, _))
          .getOrElse(queue.getConsumer(topic))
      val messages = queueConsumer.next(count)
      if (messages.size > 0) {
        broker.send(messages).sync()
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

  def request(topic: String, consumer: Option[String], count: Int, timeout: Duration): Broker[List[Message]] = this.synchronized {
    val broker = new Broker[List[Message]]
    requests = requests :+ Request(topic, consumer, count, broker, timeout, System.currentTimeMillis)
    broker
  }

  def request(topic: String, count: Int, timeout: Duration): Broker[List[Message]] = 
    request(topic, None, count, timeout)
}
