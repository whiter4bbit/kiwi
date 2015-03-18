package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.HttpResponseStatus._

import phi.Logger

object BadRequestService extends Service[Request, Response] with Logger {
  def apply(req: Request) = {
    log.warn("Bad request {}.", req)
    Future.value(Response(BAD_REQUEST))
  }
}
