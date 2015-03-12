package phi.message

case class MessageBatchWithOffset(messages: List[Message], offset: Long)


