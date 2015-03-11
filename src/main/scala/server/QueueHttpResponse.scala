package phi.server

import org.jboss.netty.handler.codec.http._
import phi.ByteChunkAndOffset

class QueueHttpResponse(version: HttpVersion, status: HttpResponseStatus, val batch: Option[ByteChunkAndOffset]) extends DefaultHttpResponse(version, status)

object QueueHttpResponse {
  val QueueHttpVersion = HttpVersion.HTTP_1_1

  def ok(batch: Option[ByteChunkAndOffset] = None): QueueHttpResponse = 
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.OK, batch)

  def ok(batch: ByteChunkAndOffset): QueueHttpResponse =
    ok(Some(batch))

  def noContent() =
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.NO_CONTENT, None)
}
