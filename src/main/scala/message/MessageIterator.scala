package phi.message

trait MessageIterator extends Iterator[Message] {
  private var _message: Option[Message] = None
  
  def getNext: Option[Message]

  private def getMessage: Option[Message] = {
    _message = _message.orElse(getNext)
    _message
  }

  def hasNext: Boolean = {
    _message.orElse(getMessage) match {
      case Some(_) => true
      case None => false
    }
  }

  def next: Message = {
    val next = _message.get
    _message = None
    next
  }
}

