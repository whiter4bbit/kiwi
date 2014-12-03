package phi

import java.nio.file.Path

sealed trait LogReader {
  def next(n: Int): List[Message]
  def next(): Option[Message] = next(1).headOption
}

class AtomicLogReader(log: Log, offset: LogOffset) extends LogReader {
  def next(n: Int): List[Message] = this.synchronized {
    val messages = log.read(offset).next(n)
    messages.lastOption.map { message =>
      offset.set(message.offset + message.payload.length)
    }
    messages
  }
}

class TxLogReader(log: Log, offset: LogOffset) extends LogReader {
  def next(n: Int): List[Message] = 
    log.read(offset).next(n)
}
