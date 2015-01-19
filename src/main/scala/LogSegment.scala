package phi

import scala.annotation.tailrec

import java.nio.channels.FileChannel
import java.nio.file.{Path => JPath}
import java.io.{RandomAccessFile, File => JFile, IOException}

import phi.io._
import phi.message.{MessageIterator, TransferableMessageSet}

class LogSegment(val file: JFile) extends Logger {
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

  def recover(): Unit = {
    try {
      channel.position(0)
      val iterator = MessageIterator(channel, 0)
      
      @tailrec def validOffset(): Long = {
        if (iterator.next) validOffset() else iterator.getPosition
      }

      val offset = validOffset()
      if (raf.length != offset) {
        log.info("Segment %s truncated to %d", file, offset)
        channel.truncate(offset)
      } else {
        log.info("Segment %s don't needs recovery.")
      }
    } catch {
      case e: Error => close(); throw e
      case e: IOException => close(); throw e
    }
  }

  def length: Long = raf.length

  def lastModified: Long = file.lastModified

  def read(offset: Long, max: Int): LogSegmentView = {
    require(this.offset <= offset, s"Given offset ${offset} is less, than segment offset ${this.offset}")

    new LogSegmentView(channel, offset - this.offset, max)
  }

  def append(payload: Array[Byte]): Unit = {
    try {
      raf.writeInt(payload.size)
      raf.write(payload)
    } catch {
      case e: IOException => close(); throw e
    }
  }

  def append(set: TransferableMessageSet): Unit = {
    try {
      set.transferTo(channel)
    } catch {
      case e: IOException => close(); throw e
    }
  }

  def flush(): Unit = {
    raf.getFD.sync
  }

  def delete(): Unit = {
    close()
    file.delete
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
