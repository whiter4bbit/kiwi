package phi.client

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.message.{MessageSet, Message}

class QueueClient(url: String) {
  private val client: Service[HttpRequest, HttpResponse] = 
    Http.newService(url)

  def producer(topic: String) = new QueueProducer(client, topic)

  def simpleConsumer(topic: String, id: String) = new SimpleQueueConsumer(client, topic, id)

  def consumer(topic: String, id: String) = new QueueConsumer(client, topic, id)

  def consumer(topic: String) = new GlobalQueueConsumer(client, topic)
}

object QueueClient {
  def apply(url: String) = new QueueClient(url)
}
