package phi.message

import java.io.File
import java.nio.ByteBuffer

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import phi.io._

case class Message(payload: Array[Byte])

object Message {
  val LengthSize = 4

  def fromBuffer(buffer: ChannelBuffer): List[Message] = {
    def read(msgs: List[Message]): List[Message] = if (buffer.readable) {
      val length = buffer.readInt
      val payload = new Array[Byte](length)
      buffer.readBytes(payload)
      read(msgs :+ Message(payload))
    } else msgs
    read(Nil)
  }

  def fromPointer(pointer: FileChannelMessagesPointer): List[Message] = {
    try {
      val buffer = ByteBuffer.allocate(pointer.region.count.toInt)
      pointer.region.channel.read(buffer, pointer.region.position)
      buffer.flip()

      fromBuffer(ChannelBuffers.wrappedBuffer(buffer))
    } catch {
      case e: Throwable => {
        throw new IllegalStateException(s"$pointer", e)
      }
    }
  }
}


