package phi.server

import com.twitter.finagle.http.{Version, Status, Response}
import org.jboss.netty.handler.codec.http.{HttpHeaders, DefaultHttpResponse}
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.channel.{FileRegion, DefaultFileRegion}

import phi.ByteChunkAndOffset
import phi.bytes.FileChannelByteChunk

trait RichResponse extends Response {
  val fileRegion: Option[FileRegion] = None
}

object RichResponse {
  def apply(chunk: ByteChunkAndOffset): RichResponse = {
    val response = new DefaultHttpResponse(Version.Http11, Status.Ok)
    response.headers.add("X-Offset", chunk.offset)

    val region = chunk.chunk match {
      case channel: FileChannelByteChunk => {
        HttpHeaders.setContentLength(response, channel.length)
        Some(new DefaultFileRegion(channel.channel, channel.offset, channel.length))
      }
      case _ => None
    }

    new RichResponse {
      val httpResponse = response
      override val fileRegion = region
    }
  }

  def apply(content: String): RichResponse = {
    val response = new DefaultHttpResponse(Version.Http11, Status.Ok)
    response.setContent(wrappedBuffer(content.getBytes))

    new RichResponse {
      val httpResponse = response
    }
  }
}
