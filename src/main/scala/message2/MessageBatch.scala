package phi.message2

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import scala.collection.Iterator

import java.nio.channels.FileChannel
import java.io.IOException

import phi.message.Message
import phi.LogFileRegion

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

class ChannelBufferMessageIterator(buffer: ChannelBuffer, lengthThreshold: Int) extends MessageIterator {
  def getNext: Option[Message] = {
    if (buffer.readableBytes >= 4) {
      val length = buffer.readInt
      if (length <= lengthThreshold && buffer.readableBytes >= length) {
        val payload = Array.ofDim[Byte](length)
        buffer.readBytes(payload)
        Some(Message(payload))
      } else None
    } else None
  }
}

class FileChannelMessageIterator(ch: FileChannel, offset: Long, max: Int, lengthThreshold: Int) extends MessageIterator {
  import java.nio.ByteBuffer

  private val lengthBuf = ByteBuffer.allocate(4)
  private var count: Int = 0

  var position: Long = offset
  
  def getNext: Option[Message] = {
    if (count < max && ch.read(lengthBuf, position) >= 4) {
      lengthBuf.flip
      val length = lengthBuf.getInt
      lengthBuf.clear

      if (length <= lengthThreshold && ch.size() - position >= length) {
        val payloadBuf = ByteBuffer.allocate(length)
        ch.read(payloadBuf, position + 4)

        val payload = Array.ofDim[Byte](length)
        payloadBuf.flip
        payloadBuf.get(payload)

        count += 1
        position += 4 + length

        Some(Message(payload))
      } else None
    } else None
  }
}

trait MessageBatch {
  def logFileRegion: Option[LogFileRegion]
  def channelBuffer: Option[ChannelBuffer]
  def transferTo(ch: FileChannel): Unit
  def iterator: Iterator[Message]
  def size: Int
}

class ChannelBufferMessageBatch private(buffer: ChannelBuffer, _size: Int, lengthThreshold: Int) extends MessageBatch {
  def logFileRegion: Option[LogFileRegion] = None
  def channelBuffer: Option[ChannelBuffer] = Some(buffer.slice)
  def transferTo(ch: FileChannel): Unit = {
    val _buffer = buffer.slice
    val _readable = buffer.readableBytes
    if (_buffer.readBytes(ch, _readable) != _readable) {
      throw new IOException(s"Can't transfer ${_buffer.readableBytes} to channel ${ch}")
    }
  }
  def iterator: Iterator[Message] = new ChannelBufferMessageIterator(buffer.copy, lengthThreshold)
  def size: Int = _size
}

object ChannelBufferMessageBatch {
  def apply(buffer: ChannelBuffer, lengthThreshold: Int = 10 * 1024 * 1024): ChannelBufferMessageBatch = {
    val _buffer = buffer.slice
    val iterator = new ChannelBufferMessageIterator(_buffer, lengthThreshold)
    val size = iterator.toList.size
    val validBuffer = buffer.slice(buffer.readerIndex, _buffer.readerIndex)
    new ChannelBufferMessageBatch(validBuffer, size, lengthThreshold)
  }
}

class FileRegionMessageBatch private(channel: FileChannel, position: Long, count: Long, _size: Int, cachedIterator: () => Iterator[Message]) extends MessageBatch {
  def logFileRegion: Option[LogFileRegion] = Some(LogFileRegion(channel, position, count))
  def channelBuffer: Option[ChannelBuffer] = None
  def transferTo(ch: FileChannel): Unit = { }
  def iterator: Iterator[Message] = cachedIterator()
  def size: Int = _size
}

object FileRegionMessageBatch {
  def apply(channel: FileChannel, offset: Long, max: Int, lengthThreshold: Int = 10 * 1024 * 1024): FileRegionMessageBatch = {
    val iterator = new FileChannelMessageIterator(channel, offset, max, lengthThreshold)
    val messages = iterator.toList
    new FileRegionMessageBatch(channel, offset, iterator.position - offset, messages.size, () => messages.iterator)
  }
}

class SimpleMessageBatch private(messages: List[Message], buffer: ChannelBuffer) extends MessageBatch {
  def logFileRegion: Option[LogFileRegion] = None
  def channelBuffer: Option[ChannelBuffer] = Some(buffer.slice)
  def transferTo(ch: FileChannel): Unit = { }
  def iterator: Iterator[Message] = messages.iterator
  def size: Int = messages.size
}

object SimpleMessageBatch {
  def apply(messages: List[Message], lengthThreshold: Int = 10 * 1024 * 1024): SimpleMessageBatch = {
    val validMessages = messages.filter(_.payload.length <= lengthThreshold)
    val buffer = ChannelBuffers.dynamicBuffer(1024)
    validMessages.foreach { message =>
      buffer.writeInt(message.payload.length)
      buffer.writeBytes(message.payload)
    }
    new SimpleMessageBatch(validMessages, buffer)
  }
}
