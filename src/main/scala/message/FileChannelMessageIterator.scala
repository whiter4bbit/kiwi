package phi.message

import java.nio.channels.FileChannel
import java.nio.ByteBuffer

class FileChannelMessageIterator(ch: FileChannel, offset: Long, max: Int, lengthThreshold: Int) extends MessageIterator {

  private val lengthBuf = ByteBuffer.allocate(4)
  private var count: Int = 0

  var position: Long = offset
  
  def getNext: Option[Message] = {
    if (count < max && ch.read(lengthBuf, position) >= 4) {
      lengthBuf.flip
      val length = lengthBuf.getInt
      lengthBuf.clear

      if (length <= lengthThreshold && ch.size() - (position + 4) >= length) {
        val payloadBuf = ByteBuffer.allocate(length)
        val readBytes = ch.read(payloadBuf, position + 4)

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

object ShallowFileChannelMessageIterator {
  val ShallowMessage = Message(Array.ofDim[Byte](1))
}

class ShallowFileChannelMessageIterator(ch: FileChannel, offset: Long, max: Int, lengthThreshold: Int) extends MessageIterator {
  import ShallowFileChannelMessageIterator._

  private val lengthBuf = ByteBuffer.allocate(4)
  private var count: Int = 0

  var position: Long = offset
  
  def getNext: Option[Message] = {
    if (count < max && ch.read(lengthBuf, position) >= 4) {
      lengthBuf.flip
      val length = lengthBuf.getInt
      lengthBuf.clear

      if (length <= lengthThreshold && ch.size() - (position + 4) >= length) {
        count += 1
        position += 4 + length

        Some(ShallowMessage)
      } else None
    } else None
  }
}

