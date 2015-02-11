package phi.server

import org.jboss.netty.handler.codec.http._
import phi.message.MessageBatchWithOffset

class QueueHttpResponse(version: HttpVersion, status: HttpResponseStatus, val batch: Option[MessageBatchWithOffset]) extends DefaultHttpResponse(version, status)

object QueueHttpResponse {
  val QueueHttpVersion = HttpVersion.HTTP_1_1

  def ok(batch: Option[MessageBatchWithOffset] = None): QueueHttpResponse = 
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.OK, batch)

  def ok(batch: MessageBatchWithOffset): QueueHttpResponse =
    ok(Some(batch))

  def noContent() =
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.NO_CONTENT, None)
}
