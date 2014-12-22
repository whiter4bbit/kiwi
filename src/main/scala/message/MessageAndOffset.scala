package phi.message

case class MessageAndOffset(offset: Long, payload: Array[Byte])
