package phi.client

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import phi.server.AppendMessage

object AppendMessageLengthPrefixEncoder {
  def apply(messages: List[AppendMessage]): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer(1024)
    messages.foreach { append => 
      buffer.writeInt(append.payload.length)
      buffer.writeBytes(append.payload)
    }
    buffer
  }
}
