package phi

import java.nio.file.Path
import java.io.{IOException, EOFException, File, RandomAccessFile}
import phi.io._

class LogOffset(baseDir: Path, topicName: String, consumer: Option[String] = None) {
  @volatile private var offset: Long = _
  private var offsetFile: File = _
  private var raf: RandomAccessFile = _

  init()

  private def init(): Unit = {
    val journalPath = baseDir.resolve(topicName)
    if (!journalPath.exists)
      journalPath.createDirectories
    val fileName = "offset" + consumer.map(c => s"-$c").getOrElse("")
    offsetFile = journalPath.resolve(fileName).toFile

    val shouldUpdate = offsetFile.exists

    raf = new RandomAccessFile(offsetFile, "rw")
    try {
      if (shouldUpdate) {
        offset = raf.readLong
      } else {
        raf.seek(0)
        raf.writeLong(0L)
      }
    } catch {
      case e: IOException => cleanup(); throw e
      case e: EOFException => cleanup(); throw e
    }
  }
  
  def get(): Long = {
    offset
  }

  def set(_offset: Long) = this.synchronized {
    offset = _offset
    
    try {
      raf.seek(0)
      raf.writeLong(_offset)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  def close(): Unit = {
    try {
      raf.seek(0)
      raf.writeLong(offset)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  private def cleanup(): Unit = {
    if (raf != null) 
      raf.close
  }
}
