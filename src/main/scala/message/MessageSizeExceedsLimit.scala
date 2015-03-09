package phi.message

case class MessageSizeExceedsLimit(limit: Int, length: Int) extends Exception
