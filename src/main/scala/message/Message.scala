package phi.message

import java.io.File
import java.nio.ByteBuffer

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.util.Try

import phi.io._

case class Message(payload: Array[Byte])

object Message {
  val LengthSize = 4

  def fromBuffer(buffer: ChannelBuffer): Try[List[Message]] = {
    def read(msgs: List[Message]): List[Message] = if (buffer.readable) {
      val length = buffer.readInt
      val payload = Array.ofDim[Byte](length)
      buffer.readBytes(payload)
      read(msgs :+ Message(payload))
    } else msgs

    Try(read(Nil))
  }

  def fromPointer(pointer: FileChannelMessagesPointer): Try[List[Message]] = {
    val buffer = ByteBuffer.allocate(pointer.region.count.toInt)
    pointer.region.channel.read(buffer, pointer.region.position)
    buffer.flip()

    fromBuffer(ChannelBuffers.wrappedBuffer(buffer))
  }
}


