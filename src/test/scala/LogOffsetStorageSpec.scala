package phi

import phi.io._
import PhiFiles._

import org.scalatest._

class LogOffsetStorageSpec extends FlatSpec with Matchers {
  "LogOffsetStorage" should "store offsets" in {
    withTempDir("offsets") { dir =>
      val storage = LogOffsetStorage.open(dir, "topic-1")
      storage.put("consumer-1", 100)
      storage.put("consumer-2", 42)

      storage.get("consumer-1") should be (Some(100))
      storage.get("consumer-2") should be (Some(42))
      storage.get("consumer-3") should be (None)
    }
  }

  "LogOffsetStorage" should "resize when need" in {
    withTempDir("offsets") { dir =>
      val storage = LogOffsetStorage.open(dir, "topic-1")

      val ConsumersCount = 10000

      (0 until ConsumersCount).foreach(i => storage.put(s"consumer-$i", i))

      val stored = (0 until ConsumersCount).map(i => storage.get(s"consumer-$i")).toList.flatten

      stored should be ((0 until ConsumersCount).toList)
    }
  }

  "LogOffsetStorage" should "correctly load stored offsets" in {
    withTempDir("offsets") { dir =>
      val storage = LogOffsetStorage.open(dir, "topic-1")
      
      val ConsumersCount = 10000

      (0 until ConsumersCount).foreach(i => storage.put(s"consumer-$i", i))

      storage.close

      val storage2 = LogOffsetStorage.open(dir, "topic-1")

      val stored = (0 until ConsumersCount).map(i => storage.get(s"consumer-$i")).toList.flatten

      stored should be ((0 until ConsumersCount).toList)
    }
  }
}
