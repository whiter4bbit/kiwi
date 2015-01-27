package phi.message

import java.io.File
import java.nio.ByteBuffer

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.util.Try

import phi.io._

case class Message(payload: Array[Byte])

object Message {
  val LengthSize = 4
}


