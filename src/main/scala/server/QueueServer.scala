package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.{Http, HttpMuxer}
import com.twitter.server.TwitterServer 
import com.twitter.util.{Await, Future}
import org.jboss.netty.handler.codec.http._
import java.net.InetSocketAddress

object QueueServer extends TwitterServer {
  def main(): Unit = {
    val handleExceptions = new ExceptionHandler(log)
    val queueService = new QueueService(".", statsReceiver) 

    val service = handleExceptions andThen queueService

    val http = Http()

    val server = ServerBuilder()
      .codec(QueueHttpCodec(http))
      .bindTo(new InetSocketAddress(8080))
      .name("queue-server")
      .reportTo(statsReceiver)
      .build(service)

    Await.ready(adminHttpServer)
  }
}
