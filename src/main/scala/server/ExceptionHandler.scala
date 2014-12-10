package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util.{Future}
import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http._

class ExceptionHandler(log: Logger) extends SimpleFilter[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest, service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
    service(req) handle {
      case e: Throwable => {
        log.error(e, "Can't handle request: %s.", req)
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR)
      }
    }
  }
}
