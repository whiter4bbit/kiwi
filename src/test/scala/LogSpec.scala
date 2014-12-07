package phi

import phi.io._
import PhiFiles._

import org.scalatest._

class LogSpec extends FlatSpec with Matchers {
  "Log" should "append messages" in {
    withTempDir("log-spec") { dir =>
      val log = new Log(dir, "topic-1")
      val messages = (0 until 10).map(i => s"message-$i".getBytes)
      messages.foreach(log.append)

      val view = log.read(new LogOffset(dir, "topic-1"))
      (0 until 10).map(_ => view.next).flatten.map(_.payload.deep) should be (messages.map(_.deep))
    }
  }
}
