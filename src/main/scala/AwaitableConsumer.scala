package phi

import java.util.concurrent.{ConcurrentSkipListSet, ConcurrentLinkedQueue}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.annotation.tailrec

import com.twitter.util.{Promise, Future, Try, Return, Throw}
import phi.bytes._
import phi.message.MessageBatchWithOffset

class AwaitableConsumer private(kiwi: Kiwi, interval: Duration) extends Logger {
  import AwaitableConsumer._

  private val awaits = new ConcurrentLinkedQueue[Await]

  private val scheduler = Executors.newSingleThreadScheduledExecutor

  private[phi] def start(): Unit = {
    scheduler.scheduleWithFixedDelay(tick, 0, interval.toMillis, TimeUnit.MILLISECONDS)
  }

  private val tick = new Runnable() {
    override def run(): Unit = {
      process()
    }
  }

  private def process(): Unit = {
    val iter = awaits.iterator
    @tailrec def iterate(): Unit = {
      if (iter.hasNext) {
        val await = iter.next
        Try(await.consumer.next(await.max)).respond {
          case ret @ Return(batch) if (batch.chunk.length > 0 || await.deadline.isOverdue) => {
            await.promise.update(ret)
            iter.remove
          }
          case th @ Throw(throwable) => {
            log.warn("Can't get messages.", throwable)
            await.promise.update(th)
            iter.remove
          }
          case _ => //pass
        }
        iterate()
      }
    }
    try {
      iterate()
    } catch {
      case throwable: Throwable => log.warn("Error while iterating.", throwable)
    }
  }

  def await(consumer: Consumer, max: Int, timeout: FiniteDuration): Future[ByteChunkAndOffset] = {
    val batch = consumer.next(max)
    if (batch.chunk.length > 0) {
      Future.value(batch)
    } else {
      val promise = new Promise[ByteChunkAndOffset]
      awaits.add(Await(consumer, max, Deadline.now + timeout, promise))
      promise
    }
  }

  def await(topic: String, consumer: String, max: Int, timeout: FiniteDuration): Future[ByteChunkAndOffset] = {
    await(kiwi.getConsumer(topic, consumer), max, timeout)
  }

  def await(topic: String, max: Int, timeout: FiniteDuration): Future[ByteChunkAndOffset] = {
    await(kiwi.getConsumer(topic), max, timeout)
  }
}

object AwaitableConsumer {
  private case class Await(val consumer: Consumer, val max: Int, val deadline: Deadline, val promise: Promise[ByteChunkAndOffset])
  
  def start(kiwi: Kiwi, interval: Duration = 100 milliseconds): AwaitableConsumer = {
    val awaitableConsumer = new AwaitableConsumer(kiwi, interval)
    awaitableConsumer.start
    awaitableConsumer
  }
}
