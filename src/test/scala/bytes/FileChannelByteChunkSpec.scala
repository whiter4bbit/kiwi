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

  it should "maitain relative position and length" in {
    withTempDir("file-channel-byte-chunk") { dir =>
      val file = (dir / "message")
      val raf = file.newRandomAccessFile("rw")
      val buffer = ChannelBuffers.dynamicBuffer(512)
      buffer.writeInt(42)
      buffer.writeInt(24)
      buffer.writeInt(56)

      buffer.readBytes(raf.getChannel, buffer.readableBytes)

      val chunk = ByteChunk(raf.getChannel, 4)
      chunk.length should be (8)

      val reader = chunk.reader
      reader.position should be (0)
      reader.readInt should be (Success(24))
      reader.position should be (4)
      reader.readInt should be (Success(56))
      reader.position should be (8)
      reader.readInt should be (Failure(Eof))
      reader.position should be (8)
    }
  }
}
