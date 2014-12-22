package phi.server

import org.jboss.netty.handler.codec.http._
import phi.message.FileChannelMessagesPointer

class QueueHttpResponse(version: HttpVersion, status: HttpResponseStatus, val pointer: Option[FileChannelMessagesPointer]) extends DefaultHttpResponse(version, status)

object QueueHttpResponse {
  val QueueHttpVersion = HttpVersion.HTTP_1_1

  def ok(pointer: Option[FileChannelMessagesPointer] = None) = 
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.OK, pointer)

  def noContent() =
    new QueueHttpResponse(QueueHttpVersion, HttpResponseStatus.NO_CONTENT, None)
}
