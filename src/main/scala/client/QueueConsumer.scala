package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http._
import com.twitter.util.{Try, Future}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.handler.codec.http._

import phi.bytes._
import phi.message.{MessageBatchWithOffset, Message}

class SimpleQueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, id: String, reader: BinaryFormatReader[Message]) {
  import QueueConsumer._

  def get(count: Int): Future[MessageBatchWithOffset] = {
    client(httpGet(s"$topic/consumer/$id/$count")) flatMap decode(reader)
  }

  def await(count: Int): Future[MessageBatchWithOffset] = {
    client(httpGet(s"$topic/consumer/$id/await/$count")) flatMap decode(reader)
  }

  def offset(offset: Long): Future[Unit] = {
    client(httpPost(s"$topic/$id/offset/$offset")).map(_ => ())
  }
}

class QueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, id: String, reader: BinaryFormatReader[Message]) {
  import QueueConsumer._

  private val consumer = new SimpleQueueConsumer(client, topic, id, reader)

  def get[A](count: Int)(f: List[Message] => A): Future[A] = {
    consumer.get(count).flatMap { batch =>
      val result = f(batch.messages)
      if (batch.messages.size > 0) {
        consumer.offset(batch.offset).map(_ => result)
      } else Future.value(result)
    }
  }

  def await[A](count: Int)(f: List[Message] => A): Future[A] = {
    consumer.await(count).flatMap { batch =>
      val result = f(batch.messages)
      if (batch.messages.size > 0) {
        consumer.offset(batch.offset).map(_ => result)
      } else Future.value(result)
    }
  }
}

class GlobalQueueConsumer private[client] (client: Service[HttpRequest, HttpResponse], topic: String, reader: BinaryFormatReader[Message]) {
  import QueueConsumer._

  def get(count: Int): Future[List[Message]] = {
    for {
      response <- client(httpGet(s"$topic/$count"))
      batch <- decode(reader)(response)
    } yield batch.messages
  }

  def await(count: Int): Future[List[Message]] = {
    for {
      response <- client(httpGet(s"$topic/await/$count"))
      batch <- decode(reader)(response)
    } yield batch.messages
  }
}

object QueueConsumer {
  case class WrongResponse(msg: String) extends Exception(msg)

  private[client] def httpGet(path: String): HttpRequest = {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path)
    request.headers.set("Accept", "application/octet-stream")
    request
  }

  private[client] def httpPost(path: String): HttpRequest = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path)
  }

  private def longHeader(headers: HttpHeaders, name: String): Try[Long] = {
    Try(headers.get(name).toLong) rescue {
      case _ => Try(throw WrongResponse(s"Header $name missing or not a long."))
    }
  }
  
  private[client] def decode(reader: BinaryFormatReader[Message])(resp: HttpResponse): Future[MessageBatchWithOffset] = {
    val batch = longHeader(resp.headers(), "X-Offset").map { offset =>
      val messages = reader.fromByteChunk(ByteChunk(resp.getContent))
      MessageBatchWithOffset(messages, offset)
    }
    Future.const(batch)
  }
}

