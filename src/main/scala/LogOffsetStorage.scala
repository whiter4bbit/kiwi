package phi

import java.io.{IOException, RandomAccessFile}
import java.nio.file.Path
import java.nio.channels.FileChannel
import java.nio.MappedByteBuffer

import scala.collection.mutable.HashMap

import phi.io._

class LogOffsetStorage private (dir: Path, topic: String, flushIntervalUpdates: Int) {
  import LogOffsetStorage._

  private var buffer: MappedByteBuffer = _
  private val entries = HashMap.empty[String, EntryPointer]
  private var storageFile: PhiPath = _
  private var updatesCount: Long = 0

  private val ResizeStep = 1024 * 1024
  private val InitialSize = 1024 * 1024
  
  private def init(): Unit = {
    val storageDir = dir / topic
    if (!storageDir.exists) {
      storageDir.createDirectories
    }

    storageFile = storageDir / "_offsets"

    var raf: RandomAccessFile = null
    try {
      val newFile = !storageFile.exists
      raf = storageFile.newRandomAccessFile("rws")
      if (newFile) {
        raf.setLength(InitialSize)
      }
      val channel = raf.getChannel
      val length = raf.length
      buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, length)
      if (newFile) {
        createHeader()
      } else {
        readEntries()
      }
    } finally {
      if (raf != null) raf.close
    }
  }

  private def createHeader(): Unit = {
    buffer.putInt(0)
  }

  private def readEntries(): Unit = {
    val count = buffer.getInt()

    (0 until count).foreach { _ : Int =>
      val keyLen = buffer.getInt()
      val key = Array.ofDim[Byte](keyLen)
      buffer.get(key)

      val valueOffset = buffer.position
      buffer.getLong()

      entries += (new String(key) -> EntryPointer(valueOffset))
    }
  }

  private def resizeIfNeed(toWrite: Int): Unit = {
    if (buffer.position + toWrite > buffer.limit) {
      buffer.force

      var raf: RandomAccessFile = null
      try {
        val position = buffer.position

        raf = storageFile.newRandomAccessFile("rws")
        val channel = raf.getChannel
        val length = raf.length
        val newLength = length + ResizeStep
        raf.setLength(newLength)
        
        buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newLength)
        buffer.position(position)
      } finally {
        if (raf != null) raf.close
      }
    }
  }

  def put(key: String, offset: Long): Unit = this.synchronized {
    entries.get(key) match {
      case Some(EntryPointer(valueOffset)) => {
        buffer.putLong(valueOffset, offset)
      }
      case None => {
        val keyBytes = key.getBytes
        resizeIfNeed(keyBytes.length + 4 + 8)

        buffer.putInt(keyBytes.length)
        buffer.put(keyBytes)

        val valueOffset = buffer.position
        buffer.putLong(offset)

        entries += (new String(key) -> EntryPointer(valueOffset))

        buffer.putInt(0, buffer.getInt(0) + 1)

        updatesCount += 1

        if (updatesCount % flushIntervalUpdates == 0) {
          flush()
        }
      }
    }
  }

  def get(key: String): Option[Long] = this.synchronized {
    entries.get(key).map { pointer =>
      buffer.getLong(pointer.valueOffset)
    }
  }

  def get(key: String, default: => Long): Long = this.synchronized {
    get(key) match {
      case Some(offset) => offset
      case None => {
        val offset = default
        put(key, offset)
        offset
      }
    }
  }

  def flush(): Unit = {
    buffer.force
  }

  def close(): Unit = {
    flush()
  }
}

object LogOffsetStorage {
  private case class EntryPointer(valueOffset: Int)

  def open(dir: Path, topic: String, flushIntervalUpdates: Int = 1000): LogOffsetStorage = {
    val storage = new LogOffsetStorage(dir, topic, flushIntervalUpdates)
    storage.init
    storage
  }
}

//

object LogOffsetStorageCrashWriter {
  val ConsumersCount = 1000

  def main(args: Array[String]): Unit = {
    val storage = LogOffsetStorage.open("offset-test" / "logs", "topic-1", 1000)

    val sleep = args(0).toInt

    def round(n: Long): Unit = {
      (0 until ConsumersCount).foreach(i => storage.put(s"consumer-$i", n))
      println(n)
      Thread.sleep(sleep)
      round(n + 1)
    }

    round(0)
  }
}

object LogOffsetStorageCrashReader {
  val ConsumersCount = 1000

  def main(args: Array[String]): Unit = {
    val storage = LogOffsetStorage.open("offset-test" / "logs", "topic-1", 1000)
    val min = args(0).toInt

    (0 until ConsumersCount).foreach { i =>
      storage.get(s"consumer-$i") match {
        case Some(x) if x < min => println(s"consumer-$i = $x")
        case None => println(s"consumer-$i = None")
        case _ => /* pass */
      }
    }
  }
}
