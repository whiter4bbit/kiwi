package phi

import com.twitter.conversions.storage._

import scala.concurrent.duration._

import phi.message.Message
import phi.io._
import PhiFiles._

import org.scalatest._

class LogSpec extends FlatSpec with Matchers {
  "Log" should "append messages" in {
    withTempDir("log-spec") { dir =>
      val log = Log.open(dir, "topic-1", 100 megabytes)
      val messages = (0 until 10).map(i => s"message-$i".getBytes).toList
      messages.foreach(log.append)

      val readMessages = log.read(0L, 10).messages

      readMessages.map(_.payload.deep) should be (messages.map(_.deep))
    }
  }

  "Log" should "rotate old segment and continue write to new" in {
    withTempDir("log-spec") { dir =>
      val log = Log.open(dir, "topic-1", 1 kilobyte)
      
      val fourBytes = Array[Byte](1,1,1,1)

      (0 until 128 * 3).foreach { _ =>
        log.append(fourBytes)
      }

      (dir / "topic-1").listFiles(LogSegment.isLogSegment).size should be (3)
    }
  }

  "Log" should "choose correct segment for given offset during read" in {
    withTempDir("log-spec") { dir =>
      val log = Log.open(dir, "topic-1", 1 kilobyte)

      val first = Array[Byte](1,1,1,1)
      val second = Array[Byte](2,2,2,2)
      val third = Array[Byte](3,3,3,3)

      (0 until 128).foreach(_ => log.append(first))
      (0 until 128).foreach(_ => log.append(second))
      (0 until 128).foreach(_ => log.append(third))

      log.read(0, 128).messages.map(_.payload.deep) should be (List.fill(128)(first.deep))
      log.read(128 * (4 + 4), 128).messages.map(_.payload.deep) should be (List.fill(128)(second.deep))
      log.read(2 * 128 * (4 + 4), 128).messages.map(_.payload.deep) should be (List.fill(128)(third.deep))

      (dir / "topic-1").listFiles(LogSegment.isLogSegment).map(_.toFile.length) should be (List(1024, 1024, 1024))
    }
  }

  "Log" should "restore corrupted segments" in {
    withTempDir("logs-spec") { dir =>
      val log1 = Log.open(dir, "topic-1", 1 kilobyte)

      val message = Array[Byte](1,1,1,1)
      (0 until (128 * 3)).foreach(_ => log1.append(message))

      log1.close

      val cleanShutdownFile = (dir / "topic-1" / Log.CleanShutdownFile).toFile

      cleanShutdownFile.exists should be (true)
      cleanShutdownFile.delete should be (true)

      def offset(count: Int): Long = (4 + message.length) * count

      def segmentOffset(n: Int): Long = 128 * n * (4 + message.length)

      def truncate(offset: Long, size: Long): Unit = {
        val raf = (dir / "topic-1" / LogSegment.fileName(offset)).newRandomAccessFile("rw")
        raf.getChannel.truncate(size)
        raf.getFD.sync
        raf.close
      }

      truncate(segmentOffset(0), offset(100) + 2)
      truncate(segmentOffset(1), offset(80) + 5)
      truncate(segmentOffset(2), offset(70) + 1)

      val log2 = Log.open(dir, "topic-1", 1 kilobyte)

      (dir / "topic-1" / LogSegment.fileName(segmentOffset(0))).toFile.length should be (offset(100))
      (dir / "topic-1" / LogSegment.fileName(segmentOffset(1))).toFile.length should be (offset(80))
      (dir / "topic-1" / LogSegment.fileName(segmentOffset(2))).toFile.length should be (offset(70))

      log2.read(segmentOffset(0), 128).messages.size should be (100)
      log2.read(segmentOffset(1), 128).messages.size should be (80)
      log2.read(segmentOffset(2), 128).messages.size should be (70)
    }
  }

  "Log" should "delete segments until predicate matches" in {
    withTempDir("log-spec") { dir =>
      val log = Log.open(dir, "topic-1", 1 kilobyte)
      val message = Array[Byte](1, 1, 1, 1)
      def segmentOffset(n: Int) = 128 * n * (4 + message.length)
      
      (0 until (128 * 5)).foreach(_ => log.append(message))

      def segments = (dir / "topic-1").listFiles(LogSegment.isLogSegment)

      segments should have length 5

      log.deleteSegments(_.offset == 0)

      segments should have length 4

      log.deleteSegments(_.offset == segmentOffset(1))

      segments should have length 3

      log.deleteSegments(_ => true)

      segments should have length 1
    }
  }

  "Log" should "read messages from closest segment if given one is empty" in {
    withTempDir("log-spec") { dir =>
      val log = Log.open(dir, "topic-1", 1 kilobytes)
      val message = Array[Byte](1, 1, 1, 1)

      (0 until (128 * 5)).foreach(_ => log.append(message))

      log.deleteSegments(_.offset == 0)

      log.read(0, 128).messages should have length 128
    }
  }

  "Log" should "read messages from next closest segment if current is truncated to empty" in {
    withTempDir("log-spec") { dir =>
      val log1 = Log.open(dir, "topic-1", 1 kilobytes)
      val message1 = Array[Byte](1, 1, 1, 1)
      val message2 = Array[Byte](2, 2, 2, 2)

      (0 until 128).foreach(_ => log1.append(message1))
      (0 until 128).foreach(_ => log1.append(message2))

      log1.close

      val cleanShutdownFile = (dir / "topic-1" / Log.CleanShutdownFile).toFile

      cleanShutdownFile.exists shouldBe true
      cleanShutdownFile.delete shouldBe true

      def truncate(offset: Long, size: Long): Unit = {
        val raf = (dir / "topic-1" / LogSegment.fileName(offset)).newRandomAccessFile("rw")
        raf.getChannel.truncate(size)
        raf.getFD.sync
        raf.close
      }

      truncate(0, 5)

      val log2 = Log.open(dir, "topic-1", 1 kilobytes)

      log2.read(0, 128).messages should have length 128
    }
  }
}
