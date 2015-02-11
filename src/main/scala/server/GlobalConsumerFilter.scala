package phi.server

import scala.concurrent.duration._

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.Kiwi

class GlobalConsumerFilter(val kiwi: Kiwi) extends BaseConsumerFilter {
  def apply(req: HttpRequest, service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] =
    (req.getMethod, Path(req.getUri)) match {
      case Get -> Root / topic  =>
        next(kiwi.getConsumer(topic), 1)
      case Get -> Root / topic / Integer(max) if max > 0 =>
        next(kiwi.getConsumer(topic), max)
      case Get -> Root / topic / "await" / Integer(max) if max > 0 => 
        await(kiwi.getConsumer(topic), max)
      case Get -> Root / topic / "await" / Integer(max) / Integer(timeout) if timeout > 0 && max > 0 =>
        await(kiwi.getConsumer(topic), max, timeout milliseconds)
      case _ => service(req)
    }
}
