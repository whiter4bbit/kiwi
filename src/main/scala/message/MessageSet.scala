package phi.message

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

sealed trait MessageSet
case class EagerMessageSet(messages: List[Message]) extends MessageSet
case class ChannelBufferMessageSet(buffer: ChannelBuffer) extends MessageSet

object MessageSet {
  def asBuffer(eager: EagerMessageSet): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer(512)
    eager.messages.foreach { message =>
      buffer.writeInt(message.payload.length)
      buffer.writeBytes(message.payload)
    }
    buffer
  }

  def asBuffer(set: MessageSet): ChannelBuffer = set match {
    case eager: EagerMessageSet => asBuffer(eager)
    case ChannelBufferMessageSet(buffer) => buffer
  }
}

