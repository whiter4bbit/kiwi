package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.handler.codec.http._

import phi.message.Message

class QueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, id: String) {
  import QueueConsumer._

  def fetch(count: Int): Future[List[MessageAndOffset]] = {
    client(get(s"$topic/consumer/$id/$count")).map(decode)
  }

  def poll(count: Int): Future[List[MessageAndOffset]] = {
    client(get(s"$topic/consumer/$id/poll/$count")).map(decode)
  }

  def commit(offset: Long): Future[Unit] = {
    client(post(s"$topic/consumer/$id/commit", wrappedBuffer(offset.toString.getBytes))).map(_ => ())
  }
}

class QueueGlobalConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String) {
  import QueueConsumer._

  def fetch(count: Int): Future[List[MessageAndOffset]] = {
    client(get(s"$topic/$count")).map(decode)
  }

  def poll(count: Int): Future[List[MessageAndOffset]] = {
    client(get(s"$topic/poll/$count")).map(decode)
  }
}

object QueueConsumer {
  private[client] def get(path: String): HttpRequest =
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path)

  private[client] def post(path: String, buffer: ChannelBuffer): HttpRequest = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path)
    request.setContent(buffer)
    request
  }
  
  private[client] def decode(resp: HttpResponse): List[MessageAndOffset] = {
    val offset = resp.headers().get("X-Start-Offset").toInt
    val messages = Message.fromBuffer(resp.getContent)
    MessageAndOffset.fromMessages(offset, messages)
  }
}

