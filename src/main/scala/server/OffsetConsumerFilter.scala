package phi.server

import scala.concurrent.duration._

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.Kiwi

class OffsetConsumerFilter(val kiwi: Kiwi) extends BaseConsumerFilter {
  def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
    val contentType = req.accept.headOption

    (req.getMethod, Path(req.getUri)) match {
      case Get -> Root / topic / "consumer" / consumer =>
        withContentType(contentType)(next(kiwi.getConsumer(topic, consumer), 1))
      case Get -> Root / topic / "consumer" / consumer / Integer(max) if max > 0 => 
        withContentType(contentType)(next(kiwi.getConsumer(topic, consumer), max))
      case Get -> Root / topic / "consumer" / consumer / "await" / Integer(max) if max > 0 => 
        withContentType(contentType)(await(kiwi.getConsumer(topic, consumer), max))
      case Get -> Root / topic / "consumer" / consumer / "await" / Integer(max) / Integer(timeout) if max > 0 && timeout > 0 =>
        withContentType(contentType)(await(kiwi.getConsumer(topic, consumer), max, timeout milliseconds))
      case _ => service(req)
    }
  }
}

