package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.Method._
import com.twitter.util.Future

import phi.{Logger, Kiwi}
import phi.json.JsonMessageFormat
import phi.bytes._

import ContentTypes._
import Exceptions._

class ProducerFilter(kiwi: Kiwi) extends SimpleFilter[Request, Response] with Logger {
  def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
    (req.getMethod, Path(req.getUri)) match {
      case Post -> Root / topic if req.contentType == Some(`application/json`) => {
        JsonMessageFormat.fromJson(req.getContent, kiwi.messageFormat) match {
          case Success(chunk) => {
            kiwi.getProducer(topic).append(chunk)
            Future.value(Response())
          }
          case Failure(Throw(e)) => Future.rawException(e)
        }
      }
      case Post -> Root / topic if req.contentType == Some(`application/octet-stream`) => {
        kiwi.getProducer(topic).append(ByteChunk(req.getContent))
        Future.value(Response())
      }
      case Post -> Root / topic => 
        Future.rawException(UnsupportedContentType(req.contentType.getOrElse("")))
      case _ => service(req)
    }
  }
}
