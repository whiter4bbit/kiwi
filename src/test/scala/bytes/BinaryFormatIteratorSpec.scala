package phi.bytes

import org.jboss.netty.buffer.ChannelBuffers
import org.scalatest._

import phi.message._
import phi.io._
import PhiFiles._

class BinaryFormatIteratorSpec extends FlatSpec with Matchers {
  val MessageSizeBytes = "message-00".getBytes.length + 4

  val messages = (0 until 10).map(i => Message(f"message-$i%02d".getBytes)).toList
  val format = MessageBinaryFormat(10)

  def builder = ByteChunk.builder(ChannelBuffers.dynamicBuffer(512))

  "BinaryFormatIterator" should "iterate over messages specified by format" in {
    val chunk = format.write(messages, builder).get
    
    val iter = BinaryFormatIterator(chunk, format)
    def count(n: Int): Int = if (iter.hasNext) { iter.next; count(n + 1); } else n
    count(0) should be (10)
  }

  it should "throw exception during iteration if non Eof failure returned" in {
    val messages = (0 until 20).map(i => Message(("x" * i).getBytes)).toList
    val chunk = MessageBinaryFormat(20).write(messages, builder).get

    intercept[MessageSizeExceedsLimit] {
      BinaryFormatIterator(chunk, format).toList
    }
  }

  it should "stop iteration in case when Eof failure returned" in {
    val chunk = format.write(messages, builder).get

    val iter1 = BinaryFormatIterator(chunk.take(8 * MessageSizeBytes + 3), format)
    iter1.toList should have length 8
    iter1.position should be (8 * MessageSizeBytes)

    val iter2 = BinaryFormatIterator(chunk.take(7 * MessageSizeBytes + 5), format)
    iter2.toList should have length 7
    iter2.position should be (7 * MessageSizeBytes)
  }
}
