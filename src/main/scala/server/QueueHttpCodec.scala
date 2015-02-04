package phi.server

import com.twitter.finagle.{Codec, CodecFactory}
import com.twitter.finagle.http.{HttpTransport, Http}
import com.twitter.finagle.http.codec.HttpServerDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Service
import com.twitter.util.Closable
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.handler.codec.http._

case class QueueHttpCodec(http: Http = Http()) extends CodecFactory[HttpRequest, HttpResponse] {
  def client = ???

  def server = { config => 
    val httpServer = http.server(config)

    new Codec[HttpRequest, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        val httpPipelineFactory = httpServer.pipelineFactory
        def getPipeline() = {
          val httpPipeline = httpPipelineFactory.getPipeline
          httpPipeline.addLast("queueHttpResponseHandler", new QueueHttpResponseHandler)
          httpPipeline
        }
      }

      override def newServerDispatcher(transport: Transport[Any, Any], service: Service[HttpRequest, HttpResponse]): Closable =
        new HttpServerDispatcher(new HttpTransport(transport), service)
     }
  }
}

