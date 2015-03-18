package phi.server

import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers
import ChannelBuffers._
import HttpResponseStatus._

import phi.ByteChunkAndOffset

class QueueHttpResponse(status: HttpResponseStatus, val batch: Option[ByteChunkAndOffset]) 
    extends DefaultHttpResponse(HttpVersion.HTTP_1_1, status)

object QueueHttpResponse {
  def ok(batch: Option[ByteChunkAndOffset] = None): QueueHttpResponse = 
    new QueueHttpResponse(OK, batch)

  def ok(batch: ByteChunkAndOffset): QueueHttpResponse =
    ok(Some(batch))

  def noContent() =
    new QueueHttpResponse(NO_CONTENT, None)

  def ok(content: String): QueueHttpResponse = {
    val response = new QueueHttpResponse(OK, None)
    response.setContent(wrappedBuffer(content.getBytes))
    response
  }
}
