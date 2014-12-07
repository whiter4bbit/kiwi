package phi.server

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

import phi.Message

object LengthPrefixEncoder {
  def apply(messages: List[Message]): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer(1024)
    messages.foreach { message =>
      buffer.writeLong(message.offset)
      buffer.writeInt(message.payload.length)
      buffer.writeBytes(message.payload)
    }
    buffer
  }
}
