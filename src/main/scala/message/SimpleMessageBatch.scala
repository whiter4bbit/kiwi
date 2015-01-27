package phi.message

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import java.nio.channels.FileChannel
import java.io.IOException

import phi.LogFileRegion

class SimpleMessageBatch private(messages: List[Message], buffer: ChannelBuffer) 
    extends MessageBatch {

  def logFileRegion: Option[LogFileRegion] = None
  def channelBuffer: Option[ChannelBuffer] = Some(buffer.slice)
  def transferTo(ch: FileChannel): Unit = {
    val _buffer = buffer.slice
    val _readable = buffer.readableBytes
    if (_buffer.readBytes(ch, _readable) != _readable) {
      throw new IOException(s"Can't transfer ${_buffer.readableBytes} to channel ${ch}")
    }
  }
  def iterator: Iterator[Message] = messages.iterator
  def count: Int = messages.size
  def sizeBytes: Long = 0
}

object SimpleMessageBatch {
  def apply(messages: List[Message], 
    lengthThreshold: Int = 10 * 1024 * 1024): SimpleMessageBatch = {

    val validMessages = messages.filter(_.payload.length <= lengthThreshold)
    val buffer = ChannelBuffers.dynamicBuffer(1024)
    validMessages.foreach { message =>
      buffer.writeInt(message.payload.length)
      buffer.writeBytes(message.payload)
    }
    new SimpleMessageBatch(validMessages, buffer)
  }
}

