package phi.client

import phi.message.Message

case class MessageAndOffset(offset: Long, payload: Array[Byte])

object MessageAndOffset {
  def fromMessages(startOffset: Long, messages: List[Message]): List[MessageAndOffset] = {
    val withOffset = messages.foldLeft((startOffset, List.empty[MessageAndOffset])) { (offsetAndMessages, message) =>
      val (offset, messages) = offsetAndMessages
      val nextOffset = offset + Message.LengthSize + message.payload.length
      (nextOffset, messages :+ MessageAndOffset(offset, message.payload))
    }
    withOffset._2
  }
}
