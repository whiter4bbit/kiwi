package phi.message2

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import java.nio.channels.FileChannel

import phi.io._
import PhiFiles._

import phi.LogFileRegion

import org.scalatest._

class FileRegionMessageBatchSpec extends FlatSpec with Matchers {
  private val message = Array.ofDim[Byte](8)
  private def newBuffer = ChannelBuffers.dynamicBuffer(1024)
  private def writeMessages(buffer: ChannelBuffer, count: Int): Unit = (0 until count).foreach { _ =>
    buffer.writeInt(message.length)
    buffer.writeBytes(message)
  }
  private def write(buffer: ChannelBuffer)(implicit dir: PhiPath): PhiPath = {
    val file = (dir / "file-region-spec")
    val b = Array.ofDim[Byte](buffer.readableBytes)
    buffer.readBytes(b)
    file.write(b)
    file
  }
  private def withChannel[A](file: PhiPath)(f: (FileChannel => A)): A = {
    val raf = file.newRandomAccessFile("rw")
    try {
      f(raf.getChannel)
    } finally raf.close
  }

  it should "provide iterator to messages" in {
    withTempDir("file-region-message-batch") { dir =>
      val buffer = newBuffer
      writeMessages(buffer, 124)
      val file = write(buffer)(dir)

      withChannel(file) { ch =>
        FileRegionMessageBatch(ch, 0, 124).iterator.toList should have length 124

        FileRegionMessageBatch(ch, 4 + message.length, 124).iterator.toList should have length 123
      }
    }
  }

  it should "skip messages having length > threshold" in {
    withTempDir("file-region-message-batch") { dir =>
      val buffer = newBuffer
      writeMessages(buffer, 124)

      val Threshold = 1024
      buffer.writeInt(Threshold + 1)
      buffer.writeBytes(Array.ofDim[Byte](Threshold + 1))

      val file = write(buffer)(dir)

      withChannel(file) { ch =>
        FileRegionMessageBatch(ch, 0, 200, Threshold).iterator.toList should have length 124
      }
    }
  }

  it should "skip incomplete messages" in {
    withTempDir("file-region-message-batch") { dir =>
      val buffer1 = newBuffer
      writeMessages(buffer1, 124)

      buffer1.writeInt(10)

      withChannel(write(buffer1)(dir)) { ch =>
        FileRegionMessageBatch(ch, 0, 200).iterator.toList should have length 124
      }

      val buffer2 = newBuffer
      writeMessages(buffer2, 124)

      buffer2.writeByte(1)

      withChannel(write(buffer2)(dir)) { ch =>
        FileRegionMessageBatch(ch, 0, 200).iterator.toList should have length 124
      }
    }
  }

  it should "read at most 'max' messages" in { 
    withTempDir("file-region-message-batch") { dir =>
      val buffer = newBuffer
      writeMessages(buffer, 124)

      withChannel(write(buffer)(dir)) { ch =>
        FileRegionMessageBatch(ch, 0, 10).iterator.toList should have length 10
        FileRegionMessageBatch(ch, 0, 0).iterator.toList should have length 0
      }
    }
  }

  it should "return LogFileRegion instance pointing to valid messages list" in {
    withTempDir("file-region-message-batch") { dir =>
      val buffer = newBuffer
      writeMessages(buffer, 124)

      buffer.writeInt(100)

      def offset(n: Int) = (message.length + 4) * n

      withChannel(write(buffer)(dir)) { ch =>
        FileRegionMessageBatch(ch, 0, 200).logFileRegion should be (Some(LogFileRegion(ch, 0, offset(124))))
      }
    }
  }
}

