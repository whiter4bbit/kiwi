package phi

import com.twitter.concurrent.{Broker, Offer}

import scala.annotation.tailrec
import scala.concurrent.duration._

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import phi.message.FileChannelMessagesPointer

class PollingConsumer private (queue: PersistentQueue, interval: Duration = 100 milliseconds) {
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

  def request(topic: String, consumer: Option[String], count: Int, timeout: FiniteDuration): Broker[FileChannelMessagesPointer] = {
    lock.lock
    try {
      val broker = new Broker[FileChannelMessagesPointer]
      requests = requests :+ Request(topic, consumer, count, broker, Deadline.now + timeout)
      broker
    } finally lock.unlock
  }

  def request(topic: String, count: Int, timeout: FiniteDuration): Broker[FileChannelMessagesPointer] = 
    request(topic, None, count, timeout)
}

object PollingConsumer {
  private case class Request(topic: String, consumer: Option[String], count: Int, broker: Broker[FileChannelMessagesPointer], deadline: Deadline)

  def start(queue: PersistentQueue, interval: Duration = 100 milliseconds): PollingConsumer = {
    val consumer = new PollingConsumer(queue, interval)
    consumer.start()
    consumer
  }
}
