package phi.bytes

import org.jboss.netty.buffer.ChannelBuffers
import org.scalatest._

import phi.io._
import PhiFiles._
import phi.message._

class FileChannelByteChunkSpec extends FlatSpec with Matchers {
  val format = MessageBinaryFormat(10)
  val messages = (0 until 10).map(_ => Message("message".getBytes)).toList

  "FileChannelByteChunk" should "read stored data" in {
    withTempDir("file-channel-byte-chunk") { dir =>
      val file = (dir / "messages")
      val raf = file.newRandomAccessFile("rw")

      val result = format.write(messages, ByteChunk.builder(ChannelBuffers.dynamicBuffer(512))).get
      result.transferTo(raf.getChannel)

      val deser1 = BinaryFormatIterator(ByteChunk(raf.getChannel, 0), format).toList
      deser1 should have size 10

      val deser2 = BinaryFormatIterator(ByteChunk(raf.getChannel, 0).take(11 * 9), format).toList
      deser2 should have size 9
    }
  }
}
