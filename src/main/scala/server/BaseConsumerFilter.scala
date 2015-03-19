package phi.server

import scala.concurrent.duration._

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.{Logger, Consumer, Kiwi, ByteChunkAndOffset}
import phi.bytes._
import phi.json._

import ContentTypes._
import Exceptions._

trait BaseConsumerFilter extends SimpleFilter[Request, Response] with Logger {
  private[server] val kiwi: Kiwi

  def next(consumer: Consumer, n: Int) = {
    Future.value(consumer.next(n))
  }

  def await(consumer: Consumer, max: Int, timeout: FiniteDuration = 5 seconds) = {
    kiwi.getAwaitableConsumer().await(consumer, max, timeout)
  }

  def withContentType(contentType: Option[String])(chunkAndOffset: Future[ByteChunkAndOffset]): Future[Response] = {
    contentType match {
      case Some(`application/json`) => chunkAndOffset.map { chunkAndOffset =>
        val json = JsonMessageFormat.toJson(chunkAndOffset.chunk, kiwi.messageFormat)
        RichResponse(json)
      }
      case Some(`application/octet-stream`) => chunkAndOffset.map(RichResponse.apply)
      case _ => Future.rawException(UnsupportedContentType(contentType.getOrElse("")))
    }
  }
}
