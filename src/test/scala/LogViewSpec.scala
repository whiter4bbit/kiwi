package phi

import phi.io._
import PhiFiles._

import org.scalatest._

class LogViewSpec extends FlatSpec with Matchers {
  "LogView" should "return at-most 'max' messages" in {
    withTempDir("log-view-spec") { dir =>
      val log = new Log(dir, "topic-1")
      val messages = (0 until 10).map(i => s"messages-$i".getBytes)
      messages.foreach(log.append)

      val offset = new LogOffset(dir, "topic-1")

      val view1 = log.read(offset)
      view1.next(5).size should be (5)

      offset.set(0)

      val view2 = log.read(offset)
      view2.next(12).size should be (10)
    }
  }
}
