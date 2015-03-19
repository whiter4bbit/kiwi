package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Future}
import org.jboss.netty.handler.codec.http._

import phi.Logger
import phi.message.MessageSizeExceedsLimit

import Exceptions._

object ExceptionHandler extends SimpleFilter[Request, Response] with Logger {
  def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
    service(req) handle {
      case m: MessageSizeExceedsLimit => {
        log.error(s"Message size exceeds limit ${m.limit}, ${m.length}.")
        Response(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST)
      }
      case e: Throwable => {
        log.error(s"Can't handle request: ${req}", e)
        Response(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR)
      }
    }
  }
}
