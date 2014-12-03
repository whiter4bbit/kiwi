package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

case class AppendFailed(reason: String) extends Exception(reason)

class QueueClient(url: String) {
  private val client: Service[HttpRequest, HttpResponse] = 
    Http.newService(url)

  def append(topic: String, payload: Array[Byte]): Future[Unit] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, topic)
    request.setContent(ChannelBuffers.wrappedBuffer(payload))
    client(request).map { resp =>
      if (resp.getStatus == HttpResponseStatus.OK) 
        ()
      else
        throw AppendFailed(s"Not ok response $resp.getStatus")
    }
  }

  def fetch(topic: String): Future[Option[Array[Byte]]] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, topic)
    client(request).map { resp =>
      resp.getStatus match {
        case HttpResponseStatus.OK => Some(resp.getContent().array())
        case _ => None
      }
    }
  }

  def poll(topic: String): Future[Option[Array[Byte]]] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, s"$topic/poll")
    client(request).map { resp =>
      resp.getStatus match {
        case HttpResponseStatus.OK => Some(resp.getContent().array())
        case _ => None
      }
    }
  }
}

object QueueClient {
  def apply(url: String) = new QueueClient(url)
}
