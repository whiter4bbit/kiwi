package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.message.Message

class QueueClient private (client: Service[HttpRequest, HttpResponse]) {
  def producer(topic: String) = new QueueProducer(client, topic)

  def simpleConsumer(topic: String, id: String) = new SimpleQueueConsumer(client, topic, id)

  def consumer(topic: String, id: String) = new QueueConsumer(client, topic, id)

  def consumer(topic: String) = new GlobalQueueConsumer(client, topic)
}

object QueueClient {
  def apply(client: Service[HttpRequest, HttpResponse]): QueueClient = new QueueClient(client)
  def apply(url: String): QueueClient = apply(Http.newService(url))
  def apply(): QueueClient = apply("localhost:5432")
}
