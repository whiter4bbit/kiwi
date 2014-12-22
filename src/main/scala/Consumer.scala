package phi

import java.nio.file.Path

import phi.message.FileChannelMessagesPointer

sealed trait Consumer {
  def next(n: Int): FileChannelMessagesPointer
}

class GlobalConsumer(log: Log, offset: LogOffset) extends Consumer {
  def next(n: Int): FileChannelMessagesPointer = this.synchronized {
    val pointer = log.read(offset, n).pointer
    if (pointer.count > 0) {
      offset.set(pointer.offset + pointer.region.count)
    }
    pointer
  }
}

class OffsetConsumer(log: Log, offset: LogOffset) extends Consumer {
  def next(n: Int): FileChannelMessagesPointer = {
    log.read(offset, n).pointer
  }
}
