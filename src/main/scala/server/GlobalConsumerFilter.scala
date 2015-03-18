package phi.server

import scala.concurrent.duration._

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.Kiwi

class GlobalConsumerFilter(val kiwi: Kiwi) extends BaseConsumerFilter {
  def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
    val contentType = req.headers.get("Accept")

    (req.getMethod, Path(req.getUri)) match {
      case Get -> Root / topic  =>
        withContentType(contentType)(next(kiwi.getConsumer(topic), 1))
      case Get -> Root / topic / Integer(max) if max > 0 =>
        withContentType(contentType)(next(kiwi.getConsumer(topic), max))
      case Get -> Root / topic / "await" / Integer(max) if max > 0 =>
        withContentType(contentType)(await(kiwi.getConsumer(topic), max))
      case Get -> Root / topic / "await" / Integer(max) / Integer(timeout) if timeout > 0 && max > 0 =>
        withContentType(contentType)(await(kiwi.getConsumer(topic), max, timeout milliseconds))
      case _ => service(req)
    }
  }
}

