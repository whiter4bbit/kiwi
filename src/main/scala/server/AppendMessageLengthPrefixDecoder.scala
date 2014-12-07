package phi.server

import org.jboss.netty.buffer.ChannelBuffer
import scala.annotation.tailrec

object AppendMessageLengthPrefixDecoder {
  def apply(buffer: ChannelBuffer): List[AppendMessage] = {
    @tailrec def read(appends: List[AppendMessage]): List[AppendMessage] = {
      if (buffer.readable) {
        val length = buffer.readInt
        val payload = new Array[Byte](length)
        buffer.readBytes(payload)
        
        read(appends :+ AppendMessage(payload))
      } else appends
    }
    read(Nil)
  }
}
