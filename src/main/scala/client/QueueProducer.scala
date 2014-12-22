package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.message.{EagerMessageSet, MessageSet, Message}

case class SendFailed(reason: String) extends Exception(reason)

class QueueProducer private[client] (client: Service[HttpRequest, HttpResponse], topic: String) {
  def send(messages: MessageSet): Future[Unit] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, topic)
    request.setContent(MessageSet.asBuffer(messages))
    client(request).flatMap { response =>
      if (response.getStatus == HttpResponseStatus.OK) {
        Future.value(())
      } else {
        Future.rawException(SendFailed(s"Can't send message set. Non-ok status returned: ${response.getStatus}"))
      }
    }
  }

  def send(message: Message): Future[Unit] = {
    send(EagerMessageSet(List(message)))
  }

  def send(payload: Array[Byte]): Future[Unit] = {
    send(Message(payload))
  }
}
