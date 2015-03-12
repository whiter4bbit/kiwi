package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.bytes._
import phi.message.{Message, MessageBinaryFormat}

class KiwiClient private (client: Service[HttpRequest, HttpResponse], maxMessageSize: Int = 1024) {
  private val format = MessageBinaryFormat(maxMessageSize)
  private val reader = BinaryFormatReader(format)
  private val writer = BinaryFormatWriter(format)

  def producer(topic: String) = new QueueProducer(client, topic, writer)
  def simpleConsumer(topic: String, id: String) = new SimpleQueueConsumer(client, topic, id, reader)
  def consumer(topic: String, id: String) = new QueueConsumer(client, topic, id, reader)
  def consumer(topic: String) = new GlobalQueueConsumer(client, topic, reader)
}

object KiwiClient {
  def apply(client: Service[HttpRequest, HttpResponse]): KiwiClient = new KiwiClient(client)
  def apply(url: String): KiwiClient = apply(Http.newService(url))
  def apply(): KiwiClient = apply("localhost:5432")
}
