package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.bytes._
import phi.message.Message

case class SendFailed(reason: String) extends Exception(reason)

class QueueProducer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, writer: BinaryFormatWriter[Message]) {
  def send(messages: List[Message]): Future[Unit] = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, topic)
    request.headers.set("Content-Type", "application/octet-stream")
    writer.toByteChunk(messages) match {
      case Success(chunk) => {
        request.setContent(chunk.toChannelBuffer)
        client(request).flatMap { response =>
          if (response.getStatus == HttpResponseStatus.OK) {
            Future.value(())
          } else {
            Future.rawException(SendFailed(s"Can't send message. Non-ok status received from server: ${response.getStatus}"))
          }
        }
      }
      case Failure(reason) => Future.rawException(SendFailed(s"Can't serialize messages, reason: ${reason}"))
    }
  }

  def send(message: Message): Future[Unit] = {
    send(List(message))
  }

  def send(payload: Array[Byte]): Future[Unit] = {
    send(Message(payload))
  }
}
