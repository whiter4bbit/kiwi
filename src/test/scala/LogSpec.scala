package phi

import phi.message.Message
import phi.io._
import PhiFiles._

import org.scalatest._

class LogSpec extends FlatSpec with Matchers {
  "Log" should "append messages" in {
    withTempDir("log-spec") { dir =>
      val log = Log.open(dir, "topic-1")
      val messages = (0 until 10).map(i => s"message-$i".getBytes).toList
      messages.foreach(log.append)

      val readMessages = log.read(0L, 10).messages

      readMessages.map(_.payload.deep) should be (messages.map(_.deep))
    }
  }

  "Log" should "recover corrupted journal" in {
    withTempDir("log-spec") { dir =>
      val log1 = Log.open(dir, "topic-1")
      val messages = (0 until 10).map(i => Array[Byte](1, 2, 3, 4)).toList
      messages.foreach(log1.append)
      log1.close

      val journalFile = (dir / "topic-1" / "journal.bin").newRandomAccessFile("rw")
      journalFile.getChannel.truncate(journalFile.length - 2)
      journalFile.close

      val log2 = Log.open(dir, "topic-1")
      
      val readMessages = log2.read(0L, 10).messages

      readMessages.size should be (9)
    }
  }
}
