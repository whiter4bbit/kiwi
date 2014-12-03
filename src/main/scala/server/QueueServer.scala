package phi.server

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Request, RequestParamMap, Method}
import com.twitter.util.{Await, Future, FuturePool}
import com.twitter.concurrent.{Broker, Offer}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers
import HttpResponseStatus.{OK, BAD_REQUEST, NO_CONTENT}
import java.nio.file.Paths
import java.util.concurrent.Executors
import phi.{Log, LogView, Topics, TopicsPoll}

class QueueService(logPath: String) extends Service[HttpRequest, HttpResponse] {
  private val topics = new Topics(Paths.get(logPath))
  private val topicsPoll = new TopicsPoll(topics)
  private val futurePool = FuturePool(Executors.newFixedThreadPool(5))

  private def respond(status: HttpResponseStatus, content: Array[Byte]): DefaultHttpResponse = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    response.setContent(ChannelBuffers.wrappedBuffer(content))
    response
  }

  private def respond(status: HttpResponseStatus, content: String): DefaultHttpResponse = 
    respond(status, content.getBytes)

  def appendMessage(req: HttpRequest, topic: String): Future[HttpResponse] = futurePool.apply {
    val content = req.getContent
    if (content == null) {
      respond(BAD_REQUEST, s"content expected")
    } else {
      topics.getTopic(topic).append(content.array)
      respond(OK, s"")
    }
  }

  def fetchMessage(req: HttpRequest, topic: String): Future[HttpResponse] = futurePool.apply {
    topics.getTopic(topic).view().next() match {
      case Some(message) => respond(OK, message.payload)
      case None => respond(NO_CONTENT, s"queue for topic $topic is empty")
    }
  }

  def fetchMessage(req: HttpRequest, topic: String, count: Int): Future[HttpResponse] = futurePool.apply {
    topics.getTopic(topic).view().next(count) match {
      case Nil => respond(NO_CONTENT, s"queue for topic $topic is empty")
      case messages => respond(OK, LengthPrefixEncoder(messages))
    }
  }

  def pollMessage(req: HttpRequest, topic: String): Future[HttpResponse] = {
    topicsPoll.request(topic).recv.sync().map {
      case Some(payload) => respond(OK, payload)
      case None => respond(NO_CONTENT, "")
    }
  }

  def apply(req: HttpRequest): Future[HttpResponse] = {
    (req.getMethod, Path(req.getUri)) match {
      case Method.Post -> Root / topic => appendMessage(req, topic)
      case Method.Get -> Root / topic => fetchMessage(req, topic)
      case Method.Get -> Root / topic / "tx" / consumer => Future.value(respond(BAD_REQUEST, s"Transactional get for topic = $topic, consumer = $consumer"))
      case Method.Post -> Root / topic / "commit" / consumer => Future.value(respond(BAD_REQUEST, s"Transactional commit for topic = $topic, consumer = $consumer"))
      case Method.Get -> Root / topic / "batch" / count => fetchMessage(req, topic, count.toInt)
      case Method.Get -> Root / topic / "poll" => pollMessage(req, topic)
      case _ => Future.value(respond(BAD_REQUEST, "Bad request"))
    }
  }
}

object QueueServer {
  case class Options(port: Int = 8080, host: String = "", logPath: String = ".")

  def parseOptions(opts: List[String], options: Options): Options = opts match {
    case "-p"::port::tail => parseOptions(tail, options.copy(port = port.toInt))
    case "-h"::host::tail => parseOptions(tail, options.copy(host = host))
    case "-d"::path::tail => parseOptions(tail, options.copy(logPath = path))
    case unexpected::tail => System.err.println(s"Unexpected argument: $unexpected"); System.exit(1); options
    case Nil => options
  }

  def main(args: Array[String]): Unit = {
    val options = parseOptions(args.toList, Options())
    val server = Http.serve(s"${options.host}:${options.port}", new QueueService(options.logPath))
    Await.ready(server)
  }
}
