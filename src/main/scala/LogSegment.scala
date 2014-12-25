package phi

import java.nio.channels.FileChannel
import java.nio.file.{Path => JPath}
import java.io.{RandomAccessFile, File => JFile, IOException}

import phi.io._
import phi.message.TransferableMessageSet

class LogSegment(file: JFile) {
  import LogSegment._

  private var channel: FileChannel = _
  private var raf: RandomAccessFile = _

  val offset = file.getName.stripSuffix(FileExtension).toLong

  private def init(): Unit = {
    try {
      raf = file.newRandomAccessFile("rw")
      channel = raf.getChannel
      raf.seek(raf.length)
    } catch {
      case e: IOException => close(); throw e
    }
  }

  def length: Long = raf.length

  def read(offset: Long, max: Int): LogSegmentView = {
    new LogSegmentView(channel, offset - this.offset, max)
  }

  def append(payload: Array[Byte]): Unit = this.synchronized {
    try {
      raf.writeInt(payload.size)
      raf.write(payload)
    } catch {
      case e: IOException => close(); throw e
    }
  }

  def append(set: TransferableMessageSet): Unit = this.synchronized {
    try {
      set.transferTo(channel)
    } catch {
      case e: IOException => close(); throw e
    }
  }

  def close(): Unit = {
    if (raf != null) raf.close
  }
}

object LogSegment {
  val FilePrefixLength = 20
  val FileExtension = ".log"

  def isLogSegment(file: JFile): Boolean = {
    ((file.getName().length == FilePrefixLength + FileExtension.size) 
      && file.getName().endsWith(FileExtension))
  }

  def open(file: JFile): LogSegment = {
    val segment = new LogSegment(file)
    segment.init
    segment
  }

  def create(dir: JPath, offset: Long): LogSegment = {
    open((dir / fileName(offset)).toFile)
  }

  def fileName(offset: Long): String = {
    val offsetStr = offset.toString
    val name = "0" * (FilePrefixLength - offsetStr.length) + offsetStr
    name + FileExtension
  }
}
