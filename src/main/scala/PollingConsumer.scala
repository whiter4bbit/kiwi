package phi

import com.twitter.concurrent.{Broker, Offer}

import scala.annotation.tailrec
import scala.concurrent.duration._

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import phi.message2.MessageBatchWithOffset

class PollingConsumer private (kiwi: Kiwi, interval: Duration = 100 milliseconds) {
  import PollingConsumer._

  private val lock = new ReentrantLock

  private var requests = List.empty[Request]

  private val scheduler = Executors.newSingleThreadScheduledExecutor

  private def start(): Unit = {
    scheduler.scheduleWithFixedDelay(tick, 0, interval.toMillis, TimeUnit.MILLISECONDS)
  }

  private val tick = new Runnable() {
    override def run(): Unit = {
      lock.lock
      try {
        requests = process(requests, List.empty)
      } finally lock.unlock
    }
  }

  @tailrec private def process(requests: List[Request], pending: List[Request]): List[Request] = requests match {
    case request::tail if request.count == 0 => process(tail, pending)
    case Request(topic, consumer, count, broker, deadline)::tail if deadline.isOverdue => {
      val queueConsumer = consumer
          .map(kiwi.getConsumer(topic, _))
          .getOrElse(kiwi.getConsumer(topic))
      broker.send(queueConsumer.next(count)).sync()
      process(tail, pending)
    }
    case (r @ Request(topic, consumer, count, broker, _))::tail => {
      val queueConsumer = consumer
          .map(kiwi.getConsumer(topic, _))
          .getOrElse(kiwi.getConsumer(topic))
      val batch = queueConsumer.next(count)
      if (batch.count > 0) {
        broker.send(batch).sync()
        process(tail, pending)
      } else {
        process(tail, pending :+ r)
      }
    }
    case Nil => pending
  }

  def request(topic: String, consumer: Option[String], count: Int, timeout: FiniteDuration): Broker[MessageBatchWithOffset] = {
    lock.lock
    try {
      val broker = new Broker[MessageBatchWithOffset]
      requests = requests :+ Request(topic, consumer, count, broker, Deadline.now + timeout)
      broker
    } finally lock.unlock
  }

  def request(topic: String, count: Int, timeout: FiniteDuration): Broker[MessageBatchWithOffset] = 
    request(topic, None, count, timeout)
}

object PollingConsumer {
  private case class Request(topic: String, consumer: Option[String], count: Int, broker: Broker[MessageBatchWithOffset], deadline: Deadline)

  def start(kiwi: Kiwi, interval: Duration = 100 milliseconds): PollingConsumer = {
    val consumer = new PollingConsumer(kiwi, interval)
    consumer.start()
    consumer
  }
}
