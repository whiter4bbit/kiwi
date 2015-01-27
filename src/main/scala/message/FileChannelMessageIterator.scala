package phi.message

import java.nio.channels.FileChannel

class FileChannelMessageIterator(ch: FileChannel, offset: Long, max: Int, 
  lengthThreshold: Int) extends MessageIterator {

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
