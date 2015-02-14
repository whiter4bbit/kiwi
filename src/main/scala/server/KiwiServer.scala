package phi.server

import com.twitter.app.Flaggable
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.{Http, HttpMuxer}
import com.twitter.server.TwitterServer 
import com.twitter.util.{Await, Future}
import com.twitter.logging.Logger
import org.jboss.netty.handler.codec.http._

import java.io.{File => JFile}
import java.nio.file.Paths
import java.net.InetSocketAddress

import phi.{Kiwi, Config}

trait ConfigSupport {
  implicit val ofConfig: Flaggable[Config] = new Flaggable[Config] {
    override def parse(path: String) = Config.fromFile(path)
    override def show(config: Config) = "[config]"
  }
}

object KiwiServer extends TwitterServer with ConfigSupport {

  val kiwiConfig = flag("config.path", Config(), "Path to config")

  def main(): Unit = {
    val config = kiwiConfig()

    val kiwi = Kiwi.start(config)

    Runtime.getRuntime.addShutdownHook(new Thread() { 
      override def run = kiwi.shutdown
    })

    val producerFilter = new ProducerFilter(kiwi)
    val offsetFilter = new OffsetFilter(kiwi)
    val offsetConsumerFilter = new OffsetConsumerFilter(kiwi)
    val globalConsumerFilter = new GlobalConsumerFilter(kiwi)

    val service = ExceptionHandler andThen producerFilter andThen 
        offsetConsumerFilter andThen globalConsumerFilter andThen 
        offsetFilter andThen BadRequestService

    val http = Http()

    val server = ServerBuilder()
      .codec(QueueHttpCodec(http))
      .bindTo(config.bindAddress)
      .name("queue-server")
      .reportTo(statsReceiver)
      .build(service)

    Await.ready(adminHttpServer)
  }
}
