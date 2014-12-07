package phi

import java.nio.file.Path

sealed trait Consumer {
  def next(n: Int): List[Message]
  def next(): Option[Message] = next(1).headOption
}

class GlobalConsumer(log: Log, offset: LogOffset) extends Consumer {
  def next(n: Int): List[Message] = this.synchronized {
    val messages = log.read(offset).next(n)
    messages.lastOption.map { message =>
      offset.set(message.offset + 4 + message.payload.length)
    }
    messages
  }
}

class OffsetConsumer(log: Log, offset: LogOffset) extends Consumer {
  def next(n: Int): List[Message] = 
    log.read(offset).next(n)
}
