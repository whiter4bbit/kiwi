package phi.server

import com.twitter.finagle.netty3.Conversions._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{DefaultFileRegion, ChannelHandlerContext, ChannelEvent, ChannelDownstreamHandler, MessageEvent, 
Channels, Channel, ChannelFutureListener, ChannelFuture}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.http._

import phi.io._

class QueueHttpResponseHandler extends ChannelDownstreamHandler {
  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit = {
    e match {
      case m: MessageEvent => m.getMessage match {
        case httpResponse: QueueHttpResponse => {
          httpResponse.batch match {
            case Some(batch) => {
              HttpHeaders.addHeader(httpResponse, "X-Offset", batch.offset)

              val logFileRegion = batch.logFileRegion.get

              HttpHeaders.setContentLength(httpResponse, logFileRegion.count)

              val responseFuture = Channels.future(e.getChannel)
              Channels.write(ctx, responseFuture, httpResponse)

              val fileRegionFuture = Channels.future(e.getChannel)
              val region = new DefaultFileRegion(logFileRegion.channel, logFileRegion.position, logFileRegion.count)
              Channels.write(ctx, fileRegionFuture, region)

              responseFuture.flatMap(_ => fileRegionFuture).proxyTo(e.getFuture)
            }
            case None => Channels.write(ctx, e.getFuture, httpResponse)
          }

        }
        case _ => ctx.sendDownstream(e)
      }
      case _ => ctx.sendDownstream(e)
    }
  }
}
