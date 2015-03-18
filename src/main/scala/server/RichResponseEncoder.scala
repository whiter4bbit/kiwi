package phi.server

import com.twitter.finagle.netty3.Conversions._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{DefaultFileRegion, ChannelHandlerContext, ChannelEvent, ChannelDownstreamHandler, MessageEvent, 
Channels, Channel, ChannelFutureListener, ChannelFuture}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.http._

import phi.Logger
import phi.io._
import phi.bytes._

class RichResponseEncoder extends ChannelDownstreamHandler with Logger {
  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit = {
    e match {
      case m: MessageEvent => m.getMessage match {
        case richResponse: RichResponse if !richResponse.fileRegion.isEmpty => {
          val responseFuture = Channels.future(e.getChannel)
          Channels.write(ctx, responseFuture, richResponse)

          val fileRegionFuture = Channels.future(e.getChannel)
          Channels.write(ctx, fileRegionFuture, richResponse.fileRegion.get)

          responseFuture.flatMap(_ => fileRegionFuture).proxyTo(e.getFuture)
        }
        case _ => ctx.sendDownstream(e)
      }
      case _ => ctx.sendDownstream(e)
    }
  }
}
