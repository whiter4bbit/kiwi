package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Http, Request, RequestParamMap, Method}
import com.twitter.finagle.builder.{ServerBuilder}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Await, Future, FuturePool}
import com.twitter.concurrent.{Broker, Offer}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import HttpResponseStatus.{OK, BAD_REQUEST, NO_CONTENT}
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import scala.concurrent.duration._

import phi.{Log, LogView, PersistentQueue, PollingConsumer, AppendMessageSet}
import phi.message.TransferableMessageSet

class QueueService(logPath: String, stats: StatsReceiver) extends Service[HttpRequest, HttpResponse] {
  private val topics = new PersistentQueue(Paths.get(logPath))
  private val pollingConsumer = PollingConsumer.start(topics)
  private val futurePool = FuturePool(Executors.newFixedThreadPool(5))
  private val appendsCounter = stats.counter("queue-service/appends")

  private def respond(status: HttpResponseStatus, content: ChannelBuffer): HttpResponse = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    response.setContent(content)
    response
  }

  private def respond(status: HttpResponseStatus, content: String): HttpResponse = 
    respond(status, ChannelBuffers.wrappedBuffer(content.getBytes))

  def appendMessage(req: HttpRequest, topic: String): Future[HttpResponse] = futurePool.apply {
    val content = req.getContent
    if (content == null) {
      respond(BAD_REQUEST, s"content expected")
    } else {
      stats.time("queue-service/batch_append_duration") {
        val messageSet = TransferableMessageSet(content)
        topics.getProducer(topic).append(messageSet)
        appendsCounter.incr(messageSet.count)
      }
      respond(OK, s"")
    }
  }

  def fetchMessages(topic: String, count: Int): Future[HttpResponse] = futurePool.apply {
    val messageSet = topics.getConsumer(topic).next(count)
    QueueHttpResponse.ok(Some(messageSet))
  }

  def fetchMessages(topic: String, consumer: String, count: Int): Future[HttpResponse] = futurePool.apply {
    val messageSet = topics.getConsumer(topic, consumer).next(count)
    QueueHttpResponse.ok(Some(messageSet))
  }

  def pollMessages(topic: String, count: Int): Future[HttpResponse] = {
    pollingConsumer.request(topic, count, 10 seconds).recv.sync().map { messageSet =>
      QueueHttpResponse.ok(Some(messageSet))
    }
  }

  def pollMessages(topic: String, consumer: String, count: Int): Future[HttpResponse] = {
    pollingConsumer.request(topic, Some(consumer), count, 10 seconds).recv.sync().map { messageSet =>
      QueueHttpResponse.ok(Some(messageSet))
    }
  }

  def commitOffset(req: HttpRequest, topic: String, consumer: String): Future[HttpResponse] = futurePool.apply {
    val offsetStr = new String(req.getContent.array)
    topics.getOffset(topic, consumer).set(offsetStr.toInt)
    respond(OK, "")
  }

  def apply(req: HttpRequest): Future[HttpResponse] = {
    (req.getMethod, Path(req.getUri)) match {
      case Method.Get -> Root / "throw" => futurePool.apply {
        throw new Error("Something went wrong")
      }
      case Method.Post -> Root / topic => appendMessage(req, topic)
      case Method.Get -> Root / topic => fetchMessages(topic, 1)
      case Method.Get -> Root / topic / count => fetchMessages(topic, count.toInt)
      case Method.Get -> Root / topic / "poll" / count => pollMessages(topic, count.toInt)
      case Method.Get -> Root / topic / "consumer" / consumer => fetchMessages(topic, consumer, 1)
      case Method.Post -> Root / topic / "consumer" / consumer / "commit" => commitOffset(req, topic, consumer)
      case Method.Get -> Root / topic / "consumer" / consumer / "poll" / count => pollMessages(topic, consumer, count.toInt)
      case Method.Get -> Root / topic / "consumer" / consumer / count => fetchMessages(topic, consumer, count.toInt)
      case _ => Future.value(respond(BAD_REQUEST, "Bad request"))
    }
  }
}
