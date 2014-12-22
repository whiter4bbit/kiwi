package phi

import org.jboss.netty.channel.DefaultFileRegion

import java.nio.channels.FileChannel
import java.io.{EOFException, IOException, File, RandomAccessFile}

import phi.message.FileRegionMessageSet

class LogView(file: File, offset: LogOffset, max: Int = 1) {
  private var raf: RandomAccessFile = _
  private var channel: FileChannel = _
  private var _offset: Long = _

  init()

  private def init(): Unit = {
    try {
      _offset = offset.get
      raf = new RandomAccessFile(file, "r")
      channel = raf.getChannel
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

  def next(max: Int): FileRegionMessageSet = this.synchronized {
    require(max > 0, "Max messages count shold be > 0")

    var count = 0
    val startOffset: Long = _offset
    try {
      while (count < max) {
        val length = raf.readInt
        _offset += 4 + length
        raf.seek(_offset)
        count += 1
      }
    } catch {
      case e: EOFException => /* do nothing */
      case e: IOException => cleanup(); throw e
    }

    FileRegionMessageSet(startOffset, _offset - startOffset, startOffset, file)
  }
}
