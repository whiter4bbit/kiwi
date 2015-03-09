package phi.bytes

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.scalatest._

import phi.io._
import PhiFiles._
import phi.message._

class ChannelBufferByteChunkSpec extends FlatSpec with Matchers {
  val format = MessageBinaryFormat(10)
  val messages = (0 until 10).map(_ => Message("message".getBytes)).toList

  "ChannelBufferByteChunk" should "read stored data" in {
    val result = format.write(messages, ByteChunk.builder(ChannelBuffers.dynamicBuffer(512)))
    result.isDefined should be (true)

    val deserialized = BinaryFormatIterator(result.get, format).toList
    deserialized should have length 10

    val deserialized2 = BinaryFormatIterator(result.get.take(11 * 9), format).toList
    deserialized2 should have length 9
  }

  it should "transfer data to file channel" in {
    val result = format.write(messages, ByteChunk.builder(ChannelBuffers.dynamicBuffer(512))).get

    withTempDir("byte-chunk") { dir =>
      val file = (dir / "messages")
      val raf = file.newRandomAccessFile("rw")
      result.transferTo(raf.getChannel)

      val deserialized = BinaryFormatIterator(ByteChunk(ChannelBuffers.wrappedBuffer(file.readAllBytes)), format).toList
      deserialized should have length 10
    }
  }
}
