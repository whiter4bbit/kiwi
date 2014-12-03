package phi

import java.nio.file.{Files, Paths, Path => JPath}
import org.scalatest._

import phi.io._
import PhiFiles._

class LogOffsetSpec extends FlatSpec with Matchers {

  "LogOffset" should "create empty offset file if not exists" in {
    withTempDir("log-offset-spec") { dir =>
      val logOffset = new LogOffset(dir, "topic-name")
      dir.resolve("topic-name").resolve("offset").exists should be (true)
    }
  }

  "LogOffset" should "persist offset" in {
    withTempDir("log-offset-spec") { dir =>
      val offset1 = new LogOffset(dir, "topic-name")
      offset1.set(100)

      val offset2 = new LogOffset(dir, "topic-name")
      offset2.get() should be (100)
    }
  }

}
