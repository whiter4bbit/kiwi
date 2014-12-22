package phi

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths}
import java.io.{File, IOException, RandomAccessFile}

import scala.collection.mutable.ListBuffer

import phi.io._
import phi.message.TransferableMessageSet

class Log private (baseDir: Path, name: String) {
  require(name.length > 0, "Name should be defined")
  require(baseDir != null, "Base directory should be defined")

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
      channel = raf.getChannel
      recover()
      raf.seek(raf.length)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  private def recover(): Unit = {
    def truncate(length: Long) = channel.truncate(length)
    def remaining = raf.length - raf.getFilePointer
    def offset = raf.getFilePointer

    def check(): Unit = {
      if (remaining > 4) {
        val length = raf.readInt
        if (remaining >= length) {
          raf.seek(offset + length)
          check()
        } else truncate(offset - 4)
      } else truncate(offset)
    }
    check()
  }

  def append(payload: Array[Byte]): Unit = this.synchronized {
    try {
      raf.writeInt(payload.size)
      raf.write(payload)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  def append(set: TransferableMessageSet): Unit = this.synchronized {
    try {
      set.transferTo(channel)
    } catch {
      case e: IOException => cleanup(); throw e
    }
  }

  def read(offset: Long, max: Int): LogSegmentView = {
    new LogSegmentView(channel, offset, max)
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

object Log {
  def open(baseDir: Path, name: String): Log = {
    val log = new Log(baseDir, name)
    log.init()
    log
  }
}
