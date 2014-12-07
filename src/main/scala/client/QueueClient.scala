package phi.client

import phi.Message
import phi.server.AppendMessage
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

case class AppendFailed(reason: String) extends Exception(reason)

class QueueClient(url: String) {
  private val client: Service[HttpRequest, HttpResponse] = 
    Http.newService(url)

  def append(topic: String, messages: List[AppendMessage]): Future[Unit] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, topic)
    request.setContent(AppendMessageLengthPrefixEncoder(messages))
    client(request).map { resp =>
      if (resp.getStatus == HttpResponseStatus.OK) 
        ()
      else
        throw AppendFailed(s"Not ok response $resp.getStatus")
    }
  }

  private def getMessages(resp: HttpResponse): List[Message] = {
    resp.getStatus match {
      case HttpResponseStatus.OK => LengthPrefixDecoder(resp.getContent)
      case _ => List()
    }
  }

  def fetch(topic: String, count: Int): Future[List[Message]] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, s"$topic/$count")
    client(request).map(getMessages)
  }

  def poll(topic: String, count: Int): Future[List[Message]] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, s"$topic/poll/$count")
    client(request).map(getMessages)
  }

  def poll(topic: String, consumer: String, count: Int): Future[List[Message]] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, s"$topic/consumer/$consumer/poll/$count")
    client(request).map(getMessages)
  }

  def commit(topic: String, consumer: String, offset: Long): Future[Unit] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, s"$topic/consumer/$consumer/commit")
    request.setContent(ChannelBuffers.wrappedBuffer(offset.toString.getBytes))
    client(request).map(_ => ())
  }

}

object QueueClient {
  def apply(url: String) = new QueueClient(url)
}

