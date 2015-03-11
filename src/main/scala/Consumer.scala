package phi

import java.nio.file.Path

import phi.bytes._

sealed trait Consumer {
  def next(n: Int): ByteChunkAndOffset
}

class GlobalConsumer(log: Log, offsetStorage: LogOffsetStorage) extends Consumer {
  private val GlobalConsumer = "_global"

  def next(n: Int): ByteChunkAndOffset = this.synchronized {
    val offset = offsetStorage.get(GlobalConsumer, 0L)

    val batch = log.read(offset, n)
    if (batch.chunk.length > 0) {
      offsetStorage.put(GlobalConsumer, batch.offset)
    }
    batch
  }
}

class OffsetConsumer(log: Log, consumer: String, offsetStorage: LogOffsetStorage) extends Consumer {
  def next(n: Int): ByteChunkAndOffset = {
    log.read(offsetStorage.get(consumer, 0L), n)
  }
}
