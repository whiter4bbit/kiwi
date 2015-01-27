package phi.message

import org.jboss.netty.buffer.ChannelBuffer

class ChannelBufferMessageIterator(buffer: ChannelBuffer, lengthThreshold: Int) 
    extends MessageIterator {

  def getNext: Option[Message] = {
    if (buffer.readableBytes >= 4) {
      val length = buffer.readInt
      if (length <= lengthThreshold && buffer.readableBytes >= length) {
        val payload = Array.ofDim[Byte](length)
        buffer.readBytes(payload)
        Some(Message(payload))
      } else None
    } else None
  }
}

