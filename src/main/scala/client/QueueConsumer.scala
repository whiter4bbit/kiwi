package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http._
import com.twitter.util.{Try, Future}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.handler.codec.http._

import phi.message.{MessageBatchWithOffset, ChannelBufferMessageBatch, Message}

class SimpleQueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, id: String) {
  import QueueConsumer._

  def fetch(count: Int): Future[MessageBatchWithOffset] = {
    client(get(s"$topic/consumer/$id/$count")) flatMap decode
  }

  def poll(count: Int): Future[MessageBatchWithOffset] = {
    client(get(s"$topic/consumer/$id/poll/$count")) flatMap decode
  }

  def offset(offset: Long): Future[Unit] = {
    client(post(s"$topic/consumer/$id/offset", wrappedBuffer(offset.toString.getBytes))).map(_ => ())
  }
}

class QueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, id: String) {
  import QueueConsumer._

  private val consumer = new SimpleQueueConsumer(client, topic, id)

  def fetch[A](count: Int)(f: List[Message] => A): Future[A] = {
    consumer.fetch(count).flatMap { batch =>
      val result = f(batch.iterator.toList)
      if (batch.count > 0) {
        consumer.offset(batch.offset).map(_ => result)
      } else Future.value(result)
    }
  }

  def poll[A](count: Int)(f: List[Message] => A): Future[A] = {
    consumer.poll(count).flatMap { batch =>
      val result = f(batch.iterator.toList)
      if (batch.count > 0) {
        consumer.offset(batch.offset).map(_ => result)
      } else Future.value(result)
    }
  }
}

class GlobalQueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String) {
  import QueueConsumer._

  def fetch(count: Int): Future[List[Message]] = {
    for {
      response <- client(get(s"$topic/$count"))
      batch <- decode(response)
    } yield batch.iterator.toList
  }

  def poll(count: Int): Future[List[Message]] = {
    for {
      response <- client(get(s"$topic/poll/$count"))
      batch <- decode(response)
    } yield batch.iterator.toList
  }

}

object QueueConsumer {
  case class WrongResponse(msg: String) extends Exception(msg)

  private[client] def get(path: String): HttpRequest =
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path)

  private[client] def post(path: String, buffer: ChannelBuffer): HttpRequest = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path)
    request.setContent(buffer)
    request
  }

  private def longHeader(headers: HttpHeaders, name: String): Try[Long] = {
    Try(headers.get(name).toLong) rescue {
      case _ => Try(throw WrongResponse(s"Header $name missing or not a long."))
    }
  }
  
  private[client] def decode(resp: HttpResponse): Future[MessageBatchWithOffset] = {
    val batch = longHeader(resp.headers(), "X-Offset").map { offset =>
      MessageBatchWithOffset(offset, ChannelBufferMessageBatch(resp.getContent))
    }
    Future.const(batch)
  }
}

