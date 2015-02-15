package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.message.Message

class KiwiClient private (client: Service[HttpRequest, HttpResponse]) {
  def producer(topic: String) = new QueueProducer(client, topic)

  def simpleConsumer(topic: String, id: String) = new SimpleQueueConsumer(client, topic, id)

  def consumer(topic: String, id: String) = new QueueConsumer(client, topic, id)

  def consumer(topic: String) = new GlobalQueueConsumer(client, topic)
}

object KiwiClient {
  def apply(client: Service[HttpRequest, HttpResponse]): KiwiClient = new KiwiClient(client)
  def apply(url: String): KiwiClient = apply(Http.newService(url))
  def apply(): KiwiClient = apply("localhost:5432")
}
