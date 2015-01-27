package phi.message

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import phi.io._
import PhiFiles._

import org.scalatest._

class ChannelBufferMessageBatchSpec extends FlatSpec with Matchers {
  private val message = Array.ofDim[Byte](8)
  private def newBuffer = ChannelBuffers.dynamicBuffer(1024)
  private def writeMessages(buffer: ChannelBuffer, n: Int): Unit = (0 until n).foreach { _ =>
    buffer.writeInt(message.length)
    buffer.writeBytes(message)
  }

  it should "provide iterator to messages" in {
    val buffer = newBuffer
    writeMessages(buffer, 124)

    val batch = ChannelBufferMessageBatch(buffer)
    batch.iterator.toList should have length 124
    batch.iterator.toList should have length 124
  }

  it should "skip messages having length > threshold" in {
    val buffer = newBuffer
    writeMessages(buffer, 124)

    val LengthThreshold = 1024

    buffer.writeInt(LengthThreshold + 1)
    buffer.writeBytes(Array.ofDim[Byte](LengthThreshold + 1))

    val batch = ChannelBufferMessageBatch(buffer, LengthThreshold)
    batch.iterator.toList should have length 124
    batch.iterator.toList should have length 124
  }

  it should "skip incomplete messages" in {
    val buffer1 = newBuffer
    writeMessages(buffer1, 124)

    buffer1.writeByte(1)
    
    ChannelBufferMessageBatch(buffer1).iterator.toList should have length 124

    val buffer2 = newBuffer
    writeMessages(buffer2, 124)
    buffer2.writeInt(10)

    ChannelBufferMessageBatch(buffer2).iterator.toList should have length 124

    val buffer3 = newBuffer
    writeMessages(buffer3, 124)
    buffer3.writeInt(10)
    buffer3.writeBytes(Array.ofDim[Byte](9))

    ChannelBufferMessageBatch(buffer3).iterator.toList should have length 124
  }

  it should "transfer valid messages to filechannel" in {
    val buffer1 = newBuffer
    writeMessages(buffer1, 124)

    buffer1.writeInt(10)

    val batch1 = ChannelBufferMessageBatch(buffer1)
    
    val writtenBytes = withTempDir("channel-buffer-message-batch-spec") { dir =>
      val file = (dir / "messages-batch")
      val raf = file.newRandomAccessFile("rw")
      val ch = raf.getChannel
      batch1.transferTo(ch)
      ch.close

      file.readAllBytes
    }

    val buffer2 = ChannelBuffers.wrappedBuffer(writtenBytes)
    
    ChannelBufferMessageBatch(buffer2).iterator.toList should have length 124
  }

}
