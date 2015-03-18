package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.{Logger, Kiwi}
import phi.json.JsonMessageFormat
import phi.bytes._

import ContentTypes._

case class UnsupportedContentType(contentType: String) extends Exception

class ProducerFilter(kiwi: Kiwi) extends SimpleFilter[Request, Response] with Logger {
  def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
    (req.getMethod, Path(req.getUri)) match {
      case Post -> Root / topic => {
        val contentType = req.headers.get("Content-Type")
        if (contentType == "application/json") {
          JsonMessageFormat.fromJson(req.getContent, kiwi.messageFormat) match {
            case Success(chunk) => {
              kiwi.getProducer(topic).append(chunk)
              Future.value(Response())
            }
            case Failure(Throw(e)) => Future.rawException(e)
          }
        } else if (contentType == "application/octet-stream") {
          kiwi.getProducer(topic).append(ByteChunk(req.getContent))
          Future.value(Response())
        } else Future.rawException(UnsupportedContentType(contentType))
      }
      case _ => service(req)
    }
  }
}
