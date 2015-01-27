package phi

import java.nio.file.Path

import phi.message2.MessageBatchWithOffset

sealed trait Consumer {
  def next(n: Int): MessageBatchWithOffset
}

class GlobalConsumer(log: Log, offsetStorage: LogOffsetStorage) extends Consumer {
  private val GlobalConsumer = "_global"

  def next(n: Int): MessageBatchWithOffset = this.synchronized {
    val offset = offsetStorage.get(GlobalConsumer, 0L)

    val batch = log.read(offset, n)
    if (batch.count > 0) {
      offsetStorage.put(GlobalConsumer, batch.offset)
    }
    batch
  }
}

class OffsetConsumer(log: Log, consumer: String, offsetStorage: LogOffsetStorage) extends Consumer {
  def next(n: Int): MessageBatchWithOffset = {
    log.read(offsetStorage.get(consumer, 0L), n)
  }
}
