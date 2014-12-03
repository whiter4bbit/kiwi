package phi

import phi.io._
import PhiFiles._

import org.scalatest._

/**
Array(109, 101, 115, 115, 97, 103, 101, 45, 48), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 49), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 50), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 51), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 52), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 53), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 54), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 55), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 56), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 57)

Array(109, 101, 115, 115, 97, 103, 101, 45, 48), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 49), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 50), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 51), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 52), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 53), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 54), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 55), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 56), 
Array(109, 101, 115, 115, 97, 103, 101, 45, 57)
**/

class LogSpec extends FlatSpec with Matchers {
  "Log" should "append messages" in {
    withTempDir("log-spec") { dir =>
      val log = new Log(dir, "topic-1")
      val messages = (0 until 10).map(i => s"message-$i".getBytes)
      messages.foreach(log.append)

      val view = log.read(new LogOffset(dir, "topic-1"))
      (0 until 10).map(_ => view.next).flatten.map(_.payload) should be (messages)
    }
  }
}
