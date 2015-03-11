package phi.server

import com.twitter.finagle.netty3.Conversions._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{DefaultFileRegion, ChannelHandlerContext, ChannelEvent, ChannelDownstreamHandler, MessageEvent, 
Channels, Channel, ChannelFutureListener, ChannelFuture}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.http._

import phi.io._
import phi.bytes._

class QueueHttpResponseHandler extends ChannelDownstreamHandler {
  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit = {
    e match {
      case m: MessageEvent => m.getMessage match {
        case httpResponse: QueueHttpResponse => {
          httpResponse.batch match {
            case Some(batch) => {
              HttpHeaders.addHeader(httpResponse, "X-Offset", batch.offset)

              HttpHeaders.setContentLength(httpResponse, batch.chunk.length)

              val responseFuture = Channels.future(e.getChannel)
              Channels.write(ctx, responseFuture, httpResponse)

              batch.chunk match {
                case fileChannel: FileChannelByteChunk => {
                  val fileRegionFuture = Channels.future(e.getChannel)
                  val region = new DefaultFileRegion(fileChannel.channel, fileChannel.offset, fileChannel.length)
                  Channels.write(ctx, fileRegionFuture, region)

                  responseFuture.flatMap(_ => fileRegionFuture).proxyTo(e.getFuture)
                }
                case notSupported => {
                  throw new IllegalStateException(s"Transfer for $notSupported byte chunk is not supported.")
                }
              }
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
