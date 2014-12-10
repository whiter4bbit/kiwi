package phi.client

import phi.server.AppendMessage
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

class AppendMessageSetSend(val content: ChannelBuffer)

object AppendMessageSetSend {
  def apply(messages: List[AppendMessage]): AppendMessageSetSend = {
    val buffer = ChannelBuffers.dynamicBuffer(512)
    messages.foreach { message =>
      buffer.writeInt(message.payload.length)
      buffer.writeBytes(message.payload)
    }
    new AppendMessageSetSend(buffer)
  }
}
