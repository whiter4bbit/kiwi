package phi.message

import org.jboss.netty.buffer.ChannelBuffer

import java.nio.channels.FileChannel

import phi.LogFileRegion

class FileRegionMessageBatch private(channel: FileChannel, offset: Long, 
  _count: Long, max: Int, _size: Int, lengthThreshold: Int) extends MessageBatch {

  def logFileRegion: Option[LogFileRegion] = 
    Some(LogFileRegion(channel, offset, _count))
  def channelBuffer: Option[ChannelBuffer] = None
  def transferTo(ch: FileChannel): Unit = { }
  def iterator: Iterator[Message] = 
    new FileChannelMessageIterator(channel, offset, max, lengthThreshold)
  def count: Int = _size
  def sizeBytes: Long = _count
}

object FileRegionMessageBatch {
  def apply(channel: FileChannel, fileOffset: Long, max: Int, 
    lengthThreshold: Int = 10 * 1024 * 1024): FileRegionMessageBatch = {

    val iterator = 
      new FileChannelMessageIterator(channel, fileOffset, max, lengthThreshold)
    val messages = iterator.toList
    val count = iterator.position - fileOffset

    new FileRegionMessageBatch(channel, fileOffset, count, max, messages.size, 
      lengthThreshold)
  }
}

