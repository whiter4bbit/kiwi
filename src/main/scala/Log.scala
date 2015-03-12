package phi

import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths}
import java.io.{File, IOException, RandomAccessFile}
import java.util.TreeMap
import java.lang.{Long => JLong}

import scala.annotation.tailrec
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._

import phi.io._
import phi.message._
import phi.bytes._
import Exceptions._

class Log private (baseDir: Path, name: String, maxSegmentSize: StorageUnit, flushIntervalMessages: Int, messageFormat: MessageBinaryFormat) extends Logger {
  require(name.length > 0, "Name should be defined")
  require(baseDir != null, "Base directory should be defined")

  val segments = new TreeMap[JLong, LogSegment]

  private val journalPath = baseDir / name

  private var messagesCount: Long = 0

  private val cleanShutdownFile = (journalPath / Log.CleanShutdownFile).toFile

  private def init(): Unit = {
    if (!journalPath.exists) {
      journalPath.createDirectories
    }

    journalPath.listFiles(LogSegment.isLogSegment).foreach { path =>
      val segment = LogSegment.open(path.toFile, messageFormat)
      segments.put(segment.offset, segment)
    }

    if (segments.isEmpty) {
      val segment = LogSegment.create(journalPath, 0L, messageFormat)
      segments.put(0L, segment)
    } else if (!cleanShutdownFile.exists) {
      log.info(s"${cleanShutdownFile} not found, starting recovery for ${name} topic segments.")
      recover()
    }

    cleanShutdownFile.delete
  }

  private def recover(): Unit = {
    segments.foreach { entry =>
      val (offset, segment) = entry
      log.info(s"Started recovery for log ${name} segment ${offset}.")
      segment.recover()
    }

    log.info("Recovery completed.")
  }

  private def lastSegment(): LogSegment = {
    segments.get(segments.lastKey)
  }

  private def maybeRotate(): Unit = {
    if (lastSegment().length >= maxSegmentSize.inBytes) {
      rotate()
    }
  }

  private def rotate(): Unit = {
    val last = lastSegment()
    last.flush

    val newOffset = last.offset + last.length
    val newSegment = LogSegment.create(journalPath, newOffset, messageFormat)

    segments.put(newOffset, newSegment)
  }

  def deleteSegments(predicate: (LogSegment => Boolean)): Unit = this.synchronized {
    val deleteable = segments.takeWhile { entry =>
      val (_, segment) = entry
      predicate(segment)
    }

    if (deleteable.size == segments.size) {
      rotate()
    }

    deleteable.foreach { entry =>
      val (_, segment) = entry
      deleteSegment(segment)
    }
  }

  private def deleteSegment(segment: LogSegment): Unit = {
    segments -= segment.offset
    segment.delete

    log.info(s"Segment ${name}-${segment.offset} (${segment.file}) deleted.")
  }

  private def maybeFlush(segment: LogSegment): Unit = {
    messagesCount += 1
    if (messagesCount % flushIntervalMessages == 0) 
      segment.flush()
  }

  def append(batch: ByteChunk): Unit = this.synchronized {
    maybeRotate()

    val lastOffset = segments.lastKey
    val segment = segments.get(lastOffset)
    val length = segment.length
    segment.append(batch)

    maybeFlush(segment)
  }

  def read(offset: Long, max: Int): ByteChunkAndOffset = this.synchronized {
    val startOffset = segments.floorKey(offset) match {
      case null => segments.higherKey(offset)
      case floorOffset => floorOffset
    }

    val iter = segments.tailMap(startOffset).keySet.iterator

    @tailrec def findNonEmptyResult(nextOffset: Long): ByteChunkAndOffset = {
      val readOffset = if (nextOffset <= offset) {
        offset
      } else {
        nextOffset - offset
      }
      val batch = segments.get(nextOffset).read(readOffset, max)
      if (batch.chunk.length > 0 || !iter.hasNext) {
        batch
      } else {
        findNonEmptyResult(iter.next)
      }
    }

    findNonEmptyResult(iter.next)
  }

  def close(): Unit = {
    try {
      segments.values.foreach(_.close)

      cleanShutdownFile.createNewFile
      log.debug("Created clean shutdown file: {}.", cleanShutdownFile)
    } catch {
      case e: IOException => {
        log.error("Can't close log", e)
        segments.values.foreach { segment =>
          swallow(segment.close)
        }
      }
    }
  }
}

object Log {
  val CleanShutdownFile = ".clean_shutdown"

  def open(baseDir: Path, name: String, messageFormat: MessageBinaryFormat, maxSegmentSize: StorageUnit = 500 megabytes, flushIntervalMessages: Int = 1000): Log = {
    val log = new Log(baseDir, name, maxSegmentSize, flushIntervalMessages, messageFormat)
    log.init()
    log
  }
}
