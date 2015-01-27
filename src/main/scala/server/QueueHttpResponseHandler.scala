package phi.server

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{DefaultFileRegion, ChannelHandlerContext, ChannelEvent, ChannelDownstreamHandler, MessageEvent, Channels, Channel}
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
              Channels.write(ctx, e.getFuture, httpResponse)
              
              val region = new DefaultFileRegion(logFileRegion.channel, logFileRegion.position, logFileRegion.count)
              Channels.write(ctx, e.getFuture, region)
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
