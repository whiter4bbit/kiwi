package phi

import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths}
import java.io.{File, IOException, RandomAccessFile}
import java.util.TreeMap

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import phi.io._
import phi.message.TransferableMessageSet

class Log private (baseDir: Path, name: String, maxSegmentSize: StorageUnit, flushIntervalMessages: Int) {
  require(name.length > 0, "Name should be defined")
  require(baseDir != null, "Base directory should be defined")

  private val segments = new TreeMap[Long, LogSegment]

  private val journalPath = baseDir / name

  private var messagesCount: Long = 0

  private def init(): Unit = {
    if (!journalPath.exists) {
      journalPath.createDirectories
    }

    journalPath.listFiles(LogSegment.isLogSegment).foreach { path =>
      val segment = LogSegment.open(path.toFile)
      segments.put(segment.offset, segment)
    }

    if (segments.isEmpty) {
      val segment = LogSegment.create(journalPath, 0L)
      segments.put(0L, segment)
    }
  }

  private def maybeRotate(): Unit = {
    val lastOffset = segments.lastKey
    val lastSegment = segments.get(lastOffset)
    if (lastSegment.length >= maxSegmentSize.inBytes) {
      val newOffset = lastOffset + lastSegment.length
      val newSegment = LogSegment.create(journalPath, newOffset)

      segments.put(newOffset, newSegment)
    }
  }

  private def maybeFlush(segment: LogSegment): Unit = {
    messagesCount += 1
    if (messagesCount % flushIntervalMessages == 0) 
      segment.flush()
  }

  def append(payload: Array[Byte]): Unit = this.synchronized {
    maybeRotate()

    val lastOffset = segments.lastKey
    val segment = segments.get(lastOffset)
    segment.append(payload)

    maybeFlush(segment)
  }

  def append(set: TransferableMessageSet): Unit = this.synchronized {
    maybeRotate() 

    val lastOffset = segments.lastKey
    val segment = segments.get(lastOffset)
    segment.append(set)

    maybeFlush(segment)
  }

  def read(offset: Long, max: Int): LogSegmentView = {
    val segmentOffset = segments.floorKey(offset)
    segments.get(segmentOffset).read(offset, max)
  }

  def close(): Unit = {
    segments.values.foreach(_.close)
  }
}

object Log {
  def open(baseDir: Path, name: String, maxSegmentSize: StorageUnit = 500 megabytes, flushIntervalMessages: Int = 1000): Log = {
    val log = new Log(baseDir, name, maxSegmentSize, flushIntervalMessages)
    log.init()
    log
  }
}
