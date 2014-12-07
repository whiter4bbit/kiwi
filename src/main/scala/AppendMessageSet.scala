package phi

import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import org.jboss.netty.buffer.ChannelBuffer

class AppendMessageSet private (bb: ByteBuffer, val count: Int) {
  def transferTo(channel: FileChannel): Unit = {
    if (count > 0) {
      channel.write(bb)
    }
  }
}

object AppendMessageSet {
  def apply(content: ChannelBuffer): AppendMessageSet = {
    def check(count: Int): Int = {
      if (content.readable) {
        val length = content.readInt
        content.skipBytes(length)
        check(count + 1)
      } else count
    }

    val count = check(0)

    val bb = content.toByteBuffer
    bb.flip
    
    new AppendMessageSet(bb, count)
  }
}
