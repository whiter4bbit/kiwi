package phi.server

import org.jboss.netty.channel.{Channels, ChannelPipeline, ChannelPipelineFactory, ChannelDownstreamHandler, ChannelEvent, MessageEvent, ChannelHandlerContext, 
DefaultFileRegion}
import org.jboss.netty.handler.codec.frame.{DelimiterBasedFrameDecoder, Delimiters}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.finagle.server.StackServer
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.netty3.Conversions._
import com.twitter.util.{Throw, Await}

import java.io.RandomAccessFile
import java.net.InetSocketAddress

object FutureHandler extends ChannelDownstreamHandler {
  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit = {
    e match {
      case message: MessageEvent => {
        val helloFuture = Channels.future(e.getChannel)
        Channels.write(ctx, helloFuture, ChannelBuffers.wrappedBuffer("hello\n".getBytes))
        val raf = new RandomAccessFile("/Users/whiter4bbit/Downloads/src-jdk.zip", "r")
        val sendFileFuture = Channels.future(e.getChannel)
        Channels.write(ctx, sendFileFuture, new DefaultFileRegion(raf.getChannel, 0, raf.length))
        helloFuture.flatMap(_ => sendFileFuture).proxyTo(e.getFuture)
      }
      case _ => ctx.sendDownstream(e)
    }
  }
}

object StringPipelineFactory extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
    pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
//    pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast("futureHandler", FutureHandler)
    pipeline
  }
}

object NettyExample {
  def main(args: Array[String]): Unit = {
    val addr = new InetSocketAddress(5432)
    val listener = Netty3Listener(StringPipelineFactory, StackServer.defaultParams)
    val echoServer = listener.listen(addr) { transport =>
      val trans = transport.cast[String, String]
/*      trans.read().flatMap(trans.write).respond { r =>
        printf("done")
        r match {
          case Throw(e) => e.printStackTrace
          case _ => //
        }
        trans.close()
      } */ trans.write("").ensure(trans.close())
    }
    Await.result(echoServer)
  }
}
