package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.http.Response
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.Logger

object BadRequestService extends Service[HttpRequest, HttpResponse] with Logger {
  val BadRequest = Future.value(Response(HttpResponseStatus.BAD_REQUEST))

  def apply(req: HttpRequest) = {
    log.warning("Bad request %s: %s", req.getMethod, req.getUri)
    BadRequest
  }
}
