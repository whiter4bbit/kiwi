package phi.message

import org.jboss.netty.buffer.ChannelBuffer

import java.nio.channels.FileChannel
import java.io.IOException

import phi.LogFileRegion

class ChannelBufferMessageBatch private(buffer: ChannelBuffer, _size: Int, 
  lengthThreshold: Int) extends MessageBatch {

  def logFileRegion: Option[LogFileRegion] = None
  def channelBuffer: Option[ChannelBuffer] = Some(buffer.slice)
  def transferTo(ch: FileChannel): Unit = {
    val _buffer = buffer.slice
    val _readable = buffer.readableBytes
    if (_buffer.readBytes(ch, _readable) != _readable) {
      throw new IOException(s"Can't transfer ${_buffer.readableBytes} to channel ${ch}")
    }
  }
  def iterator: Iterator[Message] = 
    new ChannelBufferMessageIterator(buffer.copy, lengthThreshold)
  def count: Int = _size
  def sizeBytes: Long = 0
}

object ChannelBufferMessageBatch {
  def apply(buffer: ChannelBuffer, 
    lengthThreshold: Int = 10 * 1024 * 1024): ChannelBufferMessageBatch = {

    val _buffer = buffer.slice
    val iterator = new ChannelBufferMessageIterator(_buffer, lengthThreshold)
    val size = iterator.toList.size
    val validBuffer = buffer.slice(buffer.readerIndex, _buffer.readerIndex)
    new ChannelBufferMessageBatch(validBuffer, size, lengthThreshold)
  }
}

