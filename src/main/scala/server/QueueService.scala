package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Http, Request, RequestParamMap}
import com.twitter.finagle.http.Method._
import com.twitter.finagle.builder.{ServerBuilder}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Await, Future, FuturePool}
import com.twitter.concurrent.{Broker, Offer}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import HttpResponseStatus.{OK, BAD_REQUEST, NO_CONTENT}
import java.nio.file.Paths
import java.util.concurrent.Executors
import scala.concurrent.duration._

import phi.{Log, Kiwi, PollingConsumer}
import phi.message.ChannelBufferMessageBatch

class QueueService(kiwi: Kiwi, stats: StatsReceiver) extends Service[HttpRequest, HttpResponse] {
  private val pollingConsumer = PollingConsumer.start(kiwi)
  private val futurePool = FuturePool(Executors.newFixedThreadPool(5))
  private val appendThroughputCounter = stats.counter("queue-service/append-throughput")
  private val appendRequestsCounter = stats.counter("queue-service/append-request-count")

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
      appendRequestsCounter.incr()
      stats.time("queue-service/batch_append_duration") {
        val messageBatch = ChannelBufferMessageBatch(content)
        kiwi.getProducer(topic).append(messageBatch)
        appendThroughputCounter.incr(messageBatch.count)
      }
      respond(OK, s"")
    }
  }

  def fetchMessages(topic: String, count: Int): Future[HttpResponse] = futurePool.apply {
    val messageSet = kiwi.getConsumer(topic).next(count)
    QueueHttpResponse.ok(Some(messageSet))
  }

  def fetchMessages(topic: String, consumer: String, count: Int): Future[HttpResponse] = futurePool.apply {
    val messageSet = kiwi.getConsumer(topic, consumer).next(count)
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

  def updateOffset(req: HttpRequest, topic: String, consumer: String): Future[HttpResponse] = futurePool.apply {
    val offsetStr = new String(req.getContent.array)
    kiwi.getOffsetStorage(topic).put(consumer, offsetStr.toLong)
    respond(OK, "")
  }

  def apply(req: HttpRequest): Future[HttpResponse] = {
    (req.getMethod, Path(req.getUri)) match {
      case Post -> Root / topic => appendMessage(req, topic)
      case Get -> Root / topic => fetchMessages(topic, 1)
      case Get -> Root / topic / count => fetchMessages(topic, count.toInt)
      case Get -> Root / topic / "poll" / count => pollMessages(topic, count.toInt)
      case Get -> Root / topic / "consumer" / consumer => fetchMessages(topic, consumer, 1)
      case Post -> Root / topic / "consumer" / consumer / "offset" => updateOffset(req, topic, consumer)
      case Get -> Root / topic / "consumer" / consumer / "poll" / count => pollMessages(topic, consumer, count.toInt)
      case Get -> Root / topic / "consumer" / consumer / count => fetchMessages(topic, consumer, count.toInt)
      case _ => Future.value(respond(BAD_REQUEST, "Bad request"))
    }
  }
}
