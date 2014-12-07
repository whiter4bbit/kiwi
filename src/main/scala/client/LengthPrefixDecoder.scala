package phi.client

import phi.Message
import org.jboss.netty.buffer.ChannelBuffer
import scala.annotation.tailrec

object LengthPrefixDecoder {
  def apply(buffer: ChannelBuffer): List[Message] = {
    @tailrec def read(messages: List[Message]): List[Message] = {
      if (buffer.readable()) {
        val offset = buffer.readLong
        val length = buffer.readInt
        val payload = new Array[Byte](length)
        buffer.readBytes(payload)

        read(messages :+ Message(offset, payload))
      } else messages
    }
    read(Nil)
  }
}
