package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.{Http, HttpMuxer}
import com.twitter.server.TwitterServer 
import com.twitter.util.{Await, Future}
import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http._

import java.nio.file.Paths
import java.net.InetSocketAddress

import phi.Kiwi

object QueueServer extends TwitterServer {
  def main(): Unit = {
    val kiwi = Kiwi.start(Paths.get("kiwi-topics"))

    Runtime.getRuntime.addShutdownHook(new Thread() { override def run = kiwi.shutdown })

    val producerFilter = new ProducerFilter(kiwi)
    val offsetFilter = new OffsetFilter(kiwi)
    val offsetConsumerFilter = new OffsetConsumerFilter(kiwi)
    val globalConsumerFilter = new GlobalConsumerFilter(kiwi)
    val handleExceptions = new ExceptionHandler(log)

    val service = handleExceptions andThen producerFilter andThen 
        offsetConsumerFilter andThen globalConsumerFilter andThen 
        offsetFilter andThen BadRequestService

    val http = Http()

    val server = ServerBuilder()
      .codec(QueueHttpCodec(http))
      .bindTo(new InetSocketAddress(5432))
      .name("queue-server")
      .reportTo(statsReceiver)
      .build(service)

    Await.ready(adminHttpServer)
  }
}
