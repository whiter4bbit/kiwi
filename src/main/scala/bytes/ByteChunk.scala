package phi.bytes

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import java.nio.channels.FileChannel
import java.nio.ByteBuffer

sealed trait FailureReason
case object Eof extends FailureReason
case class Throw(throwable: Throwable) extends FailureReason

sealed trait Result[+T] {
  def map[A](f: T => A): Result[A] = this match {
    case Success(value) => Success(f(value))
    case failure: Failure => failure
  }
  def flatMap[A](f: T => Result[A]): Result[A] = this match {
    case Success(value) => f(value)
    case failure: Failure => failure
  }
  def isEmpty: Boolean
  def isDefined: Boolean = !isEmpty
  def get: T
}
case class Success[T](value: T) extends Result[T] {
  def isEmpty = false
  def get = value
}
case class Failure(reason: FailureReason) extends Result[Nothing] {
  def isEmpty = true
  def get = throw new Error("Empty result")
}

trait BinaryFormat[M] {
  def read(reader: ByteChunkReader): Result[M]
  def write(entries: List[M], builder: ByteChunkBuilder): Result[ByteChunk]
}

trait ByteChunkReader {
  def readInt: Result[Int]
  def readBytes(bytes: Array[Byte]): Result[Array[Byte]]
  def position: Long
}

trait ByteChunk {
  def reader(): ByteChunkReader
  val length: Long
  def take(n: Long): ByteChunk
  def transferTo(ch: FileChannel): Long
  def toChannelBuffer: ChannelBuffer
}

trait ByteChunkBuilder {
  def writeInt(value: Int): Result[Unit]
  def writeBytes(bytes: Array[Byte]): Result[Unit]
  def build: ByteChunk
}

class ChannelBufferByteChunk(buffer: ChannelBuffer, val length: Long) extends ByteChunk {
  class Reader(buffer: ChannelBuffer) extends ByteChunkReader {
    def readInt = try {
      Success(buffer.readInt)
    } catch {
      case _: IndexOutOfBoundsException => Failure(Eof)
    }

    def readBytes(bytes: Array[Byte]) = try {
      buffer.readBytes(bytes)
      Success(bytes)
    } catch {
      case _: IndexOutOfBoundsException => Failure(Eof)
    }

    def position = buffer.readerIndex
  }

  def reader() = {
    new Reader(buffer.slice.copy)
  }

  def take(n: Long) = {
    val slice = buffer.slice(0, n.toInt).copy
    new ChannelBufferByteChunk(slice, slice.readableBytes)
  }

  def transferTo(ch: FileChannel) = {
    buffer.slice.readBytes(ch, length.toInt)
  }

  def toChannelBuffer: ChannelBuffer = {
    buffer.slice
  }
}

class FileChannelByteChunk(val channel: FileChannel, val offset: Long, val length: Long) extends ByteChunk {
  class Reader(var _position: Long) extends ByteChunkReader {
    private val intBuf = ByteBuffer.allocate(4)

    private def checkLength: Boolean = 
      _position <= (offset + length)
    
    def readInt = try {
      if (checkLength && channel.read(intBuf, _position) == 4) {
        intBuf.flip
        val value = intBuf.getInt
        intBuf.clear

        _position += 4
        Success(value)
      } else Failure(Eof)
    } catch {
      case exception: Exception => Failure(Throw(exception))
    }

    def readBytes(bytes: Array[Byte]) = try {
      if (checkLength && channel.size() - _position >= bytes.size) {
        val buffer = ByteBuffer.allocate(bytes.size)
        if (channel.read(buffer, _position) < bytes.size) {
          Failure(Eof)
        } else {
          buffer.flip
          buffer.get(bytes)
          _position += bytes.size
          Success(bytes)
        }
      } else Failure(Eof)
    } catch {
      case exception: Exception => Failure(Throw(exception))
    }

    def position = _position - offset
  }

  def reader() = new Reader(offset)

  def take(n: Long) = {
    if (n >= 0) {
      new FileChannelByteChunk(channel, offset, n)
    } else this
  }

  def transferTo(ch: FileChannel) = {
    channel.transferTo(offset, length, ch)
  }

  def toChannelBuffer: ChannelBuffer = ???
}

class ChannelBufferByteChunkBuilder(buffer: ChannelBuffer) extends ByteChunkBuilder {
  def writeInt(value: Int) = try {
    buffer.writeInt(value)
    Success(())
  } catch {
    case _: IndexOutOfBoundsException => Failure(Eof)
  }
  def writeBytes(bytes: Array[Byte]) = try {
    buffer.writeBytes(bytes)
    Success(())
  } catch {
    case _: IndexOutOfBoundsException => Failure(Eof)
  }
  def build = {
    val slice = buffer.slice
    new ChannelBufferByteChunk(slice, slice.readableBytes)
  }
}

object ByteChunk {
  def apply(buffer: ChannelBuffer): ByteChunk = 
    new ChannelBufferByteChunk(buffer, buffer.readableBytes)

  def apply(): ByteChunk = 
    apply(ChannelBuffers.dynamicBuffer(512))

  def apply(channel: FileChannel, position: Long): ByteChunk = 
    new FileChannelByteChunk(channel, position, channel.size - position)

  def builder(buffer: ChannelBuffer): ByteChunkBuilder = 
    new ChannelBufferByteChunkBuilder(buffer)

  def builder(): ByteChunkBuilder = 
    builder(ChannelBuffers.dynamicBuffer(512))
}

class BinaryFormatIterator[M](chunk: ByteChunk, format: BinaryFormat[M]) extends Iterator[M] {
  private val reader = chunk.reader()
  private var message: Option[M] = None

  var position: Long = reader.position

  def getNext: Option[M] = {
    format.read(reader) match {
      case Success(message) => {
        position = reader.position
        Some(message)
      }
      case Failure(Eof) => None
      case Failure(Throw(throwable)) => throw throwable
    }
  }

  def next = {
    val next = message.orElse(getNext).get
    message = None
    next
  }

  def hasNext = {
    message = message.orElse(getNext)
    !message.isEmpty
  }
}

object BinaryFormatIterator {
  def apply[M](chunk: ByteChunk, format: BinaryFormat[M]) = 
    new BinaryFormatIterator(chunk, format)
}

class BinaryFormatWriter[M](format: BinaryFormat[M], newBuilder: => ByteChunkBuilder) {
  def toByteChunk(m: List[M]): Result[ByteChunk] = format.write(m, newBuilder)
}

object BinaryFormatWriter {
  def apply[M](format: BinaryFormat[M], newBuilder: => ByteChunkBuilder): BinaryFormatWriter[M] = 
    new BinaryFormatWriter(format, newBuilder)

  def apply[M](format: BinaryFormat[M]): BinaryFormatWriter[M]  = 
    new BinaryFormatWriter(format, ByteChunk.builder)
}

case class BinaryFormatReader[M](format: BinaryFormat[M]) {
  def fromByteChunk(chunk: ByteChunk): List[M] = {
    val iterator = new BinaryFormatIterator(chunk, format)
    iterator.toList
  }
}
