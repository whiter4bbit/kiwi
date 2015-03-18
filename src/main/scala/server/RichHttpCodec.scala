package phi.server

import com.twitter.finagle.{Codec, CodecFactory}
import com.twitter.finagle.http.{HttpTransport, Http, RichHttp => RichHttp, Request, Response}
import com.twitter.finagle.http.codec.HttpServerDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Service
import com.twitter.util.Closable
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.handler.codec.http._

case class RichHttpCodec(http: Http = Http()) extends CodecFactory[Request, Response] {
  val richHttp = RichHttp[Request](http)

  def client = ???

  def server = { config => 
    val httpServer = richHttp.server(config)

    new Codec[Request, Response] {
      def pipelineFactory = new ChannelPipelineFactory {
        val httpPipelineFactory = httpServer.pipelineFactory
        def getPipeline() = {
          val httpPipeline = httpPipelineFactory.getPipeline
          httpPipeline.addLast("richResponseEncoder", new RichResponseEncoder)
          httpPipeline
        }
      }

      override def newServerDispatcher(transport: Transport[Any, Any], service: Service[Request, Response]): Closable =
        new HttpServerDispatcher(new HttpTransport(transport), service)
     }
  }
}


