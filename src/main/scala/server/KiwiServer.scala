package phi.server

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.{Http, HttpMuxer}
import com.twitter.util.{Await, Future}
import org.jboss.netty.handler.codec.http._
import org.slf4j.bridge.SLF4JBridgeHandler

import java.util.logging.{Logger => JULLogger}

import phi.{Kiwi, Config}

object KiwiServer {
  case class Options(config: Option[Config] = None)

  def parse(args: List[String], options: Options = Options()): Option[Options] = {
    def help(): Option[Options] = { println(s"Usage: ${getClass.getName} [--config path]"); None }

    args match {
      case "--config"::path::tail => {
        val config = Config.fromFile(path)
        parse(tail, options.copy(config = Some(config)))
      }
      case "--help"::tail => help
      case Nil => Some(options)
      case unknown::_ => {
        println(s"Unknown option ${unknown}")
        help
      }
    }
  }

  def main(args: Array[String]): Unit = parse(args.toList) match {
    case Some(options) => run(options)
    case None => System.exit(1)
  }

  def run(options: Options): Unit = {
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()

    val config = options.config.getOrElse(Config())

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
        offsetFilter andThen  BadRequestService 

    val server = ServerBuilder()
      .codec(RichHttpCodec(Http()))
      .bindTo(config.bindAddress)
      .name("queue-server")
      .logger(JULLogger.getLogger("com.twitter.finagle"))
      .build(service)
  }
}
