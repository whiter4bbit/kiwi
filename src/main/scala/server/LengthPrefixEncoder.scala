package phi.server

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

import phi.Message

object LengthPrefixEncoder {
  def apply(messages: List[Message]): Array[Byte] = {
    val buffer = ChannelBuffers.dynamicBuffer(1024)
    messages.foreach { message =>
      buffer.writeInt(message.payload.length)
      buffer.writeBytes(message.payload)
    }
    buffer.array()
  }
}
