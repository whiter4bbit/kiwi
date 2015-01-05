package phi.message

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.Try

case class MessageAndOffset(offset: Long, nextOffset: Long, payload: Array[Byte])

object MessageAndOffset {
  private def fromMessages(startOffset: Long)(messages: List[Message]): List[MessageAndOffset] = {
    val withOffset = messages.foldLeft((startOffset, List.empty[MessageAndOffset])) { (offsetAndMessages, message) =>
      val (offset, messages) = offsetAndMessages
      val nextOffset = offset + Message.LengthSize + message.payload.length
      (nextOffset, messages :+ MessageAndOffset(offset, nextOffset, message.payload))
    }
    withOffset._2
  }

  def fromBuffer(startOffset: Long, buffer: ChannelBuffer): Try[List[MessageAndOffset]] = {
    Message.fromBuffer(buffer).map(fromMessages(startOffset))
  }
}

