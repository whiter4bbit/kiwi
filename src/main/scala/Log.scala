package phi

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths}
import java.io.{File, EOFException, IOException, RandomAccessFile}

import scala.collection.mutable.ListBuffer

import phi.io._

class LogView(file: File, offset: LogOffset) {
  private var raf: RandomAccessFile = _
  private var _offset: Long = _

  init()

  private def init(): Unit = {
    try {
      _offset = offset.get
      raf = new RandomAccessFile(file, "r")
      raf.seek(_offset)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }
  
  private def cleanup(): Unit = {
    if (raf != null) {
      raf.close
    }
  }

  def close(): Unit = {
    cleanup()
  }

  def next(): Option[Message] = 
    next(1).headOption

  def next(max: Int): List[Message] = this.synchronized {
    require(max > 0, "Max messages count should be > 0")

    var buffer = ListBuffer.empty[Message]

    try {
      while (buffer.length < max) {
        val length = raf.readInt
        val payload = new Array[Byte](length)
        if (raf.read(payload) != length) {
          raf.seek(_offset)
          throw new EOFException()
        } else {
          buffer += Message(_offset, payload)
          _offset += length + 4
        }
      }

      buffer.toList
    } catch {
      case e: EOFException => buffer.toList
      case e: IOException => cleanup(); throw e
    }
  }
}

class Log(baseDir: Path, name: String) {
  require(name.length > 0, "Name should be defined")
  require(baseDir != null, "Base directory should be defined")

  init()

  private var raf: RandomAccessFile = _
  private var channel: FileChannel = _
  private var journalFile: File = _

  private def init(): Unit = {
    val journalPath = baseDir.resolve(name)

    if (!journalPath.exists) {
      journalPath.createDirectories
    }

    try {
      journalFile = journalPath.resolve("journal.bin").toFile
      raf = new RandomAccessFile(journalFile, "rw")
      raf.seek(raf.length)
      channel = raf.getChannel
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  def append(payload: Array[Byte]): Unit = this.synchronized {
    try {
      raf.writeInt(payload.size)
      raf.write(payload)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  def append(set: AppendMessageSet): Unit = this.synchronized {
    try {
      set.transferTo(channel)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  def read(offset: LogOffset): LogView = {
    new LogView(journalFile, offset)
  }

  def close(): Unit = {
    cleanup()
  }

  private def cleanup(): Unit = {
    if (raf != null) {
      raf.close
    }
  }
}
