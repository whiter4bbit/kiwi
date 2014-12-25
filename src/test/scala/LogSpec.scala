package phi

import com.twitter.conversions.storage._

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
}
