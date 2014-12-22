package phi.message

import java.nio.channels.FileChannel
import java.nio.ByteBuffer

trait MessageIterator {
  def next: Boolean
  def getLength: Int
  def getPayload: Array[Byte]
  def getOffset: Long
  def getPosition: Long
}

class FileChannelIterator private[message] (channel: FileChannel, _position: Long) extends MessageIterator {
  private val lengthBuf = ByteBuffer.allocate(4)
  private var lengthAndPayloadOffset: Option[(Int, Long)] = None
  private var position = _position

  private def nextMessage: Option[(Int, Long)] = {
    if (channel.read(lengthBuf, position) == 4) {
      lengthBuf.flip
      val length = lengthBuf.getInt
      lengthBuf.clear

      if (channel.size() >= position + 4 + length) {
        val lengthAndPayloadOffset = Some((length, position + 4))
        position += 4 + length
        lengthAndPayloadOffset
      } else None
    } else None
  }

  def next: Boolean = {
    lengthAndPayloadOffset = nextMessage
    lengthAndPayloadOffset match {
      case Some(_) => true
      case None => false
    }
  }

  def getLength: Int = lengthAndPayloadOffset match {
    case Some((length, _)) => length
    case None => throw new Error("'next' method should be called prior to getLength call")
  }

  def getPayload: Array[Byte] = lengthAndPayloadOffset match {
    case Some((length, offset)) => {
      val payloadBuf = ByteBuffer.allocate(length)
      if (channel.read(payloadBuf, offset) == length) {
        payloadBuf.flip
        val payload = Array.ofDim[Byte](length)
        payloadBuf.get(payload)

        payload
      } else throw new Error("unexpected number of available bytes")
    }
    case _ => throw new Error("'next' method should be called prior to getPayload call")
  }

  def getOffset: Long = lengthAndPayloadOffset match {
    case Some((_, offset)) => offset - 4
    case _ => throw new Error("'next' method should be called prior to getOffset call")
  }

  def getPosition: Long = position

}

object MessageIterator {
  def apply(channel: FileChannel, position: Long): MessageIterator = 
    new FileChannelIterator(channel, position)
}
