package phi.server

import org.jboss.netty.handler.codec.http._
import phi.message2.MessageBatchWithOffset

class QueueHttpResponse(version: HttpVersion, status: HttpResponseStatus, val batch: Option[MessageBatchWithOffset]) extends DefaultHttpResponse(version, status)

object QueueHttpResponse {
  val QueueHttpVersion = HttpVersion.HTTP_1_1

  def ok(batch: Option[MessageBatchWithOffset] = None) = 
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.OK, batch)

  def noContent() =
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.NO_CONTENT, None)
}
