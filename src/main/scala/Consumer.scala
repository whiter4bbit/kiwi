package phi

import java.nio.file.Path

import phi.message.FileChannelMessagesPointer

sealed trait Consumer {
  def next(n: Int): FileChannelMessagesPointer
}

class GlobalConsumer(log: Log, offsetStorage: LogOffsetStorage) extends Consumer {
  val GlobalConsumer = "_global"
  def next(n: Int): FileChannelMessagesPointer = this.synchronized {
    val offset = offsetStorage.get(GlobalConsumer, 0L)

    val pointer = log.read(offset, n).pointer
    if (pointer.count > 0) {
      offsetStorage.put(GlobalConsumer, pointer.offset + pointer.region.count)
    }
    pointer
  }
}

class OffsetConsumer(log: Log, consumer: String, offsetStorage: LogOffsetStorage) extends Consumer {
  def next(n: Int): FileChannelMessagesPointer = {
    log.read(offsetStorage.get(consumer, 0L), n).pointer
  }
}
