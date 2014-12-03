package phi

import com.twitter.concurrent.{Broker, Offer}
import scala.collection.immutable.Queue
import scala.concurrent.duration._

class TopicsPoll(topics: Topics) { 
  case class PollRequest(topic: String, broker: Broker[Option[Array[Byte]]], timeout: Duration, timestamp: Long)

  private var requests = List.empty[PollRequest]
  @volatile private var running = false

  private class PollThread extends Thread {
    override def run(): Unit = {
      while (TopicsPoll.this.running) {
        val _requests = TopicsPoll.this.synchronized {
          val now = System.currentTimeMillis

          TopicsPoll.this.requests.filter { request =>
            if (request.timestamp + request.timeout.toMillis < now) {
              request.broker.send(None).sync()
              false
            } else true
          }
        }

        var notCompleted = List.empty[PollRequest]

        _requests.foreach { request =>
          val maybeMessage = topics.getTopic(request.topic).view().next
          maybeMessage match {
            case Some(message) => request.broker.send(Some(message.payload)).sync()
            case None => notCompleted = notCompleted :+ request
          }
        }

        TopicsPoll.this.synchronized { //this sucks, because request can be changed
          TopicsPoll.this.requests = notCompleted
        }

        Thread.sleep(200)
      }
    }
  }

  private def start(): Unit = {
    running = true

    val pollThread = new PollThread()
    pollThread.setName("persistent-queue-poll-thread")
    pollThread.setDaemon(true)
    pollThread.start()
  }

  start()

  def request(topic: String): Broker[Option[Array[Byte]]] = this.synchronized {
    val broker = new Broker[Option[Array[Byte]]]
    requests = requests :+ PollRequest(topic, broker, 1 minute, System.currentTimeMillis)
    broker
  }

}
