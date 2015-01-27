package phi.message

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import java.nio.channels.FileChannel

import phi.LogFileRegion

trait MessageBatch {
  def logFileRegion: Option[LogFileRegion]
  def channelBuffer: Option[ChannelBuffer]
  def transferTo(ch: FileChannel): Unit
  def iterator: Iterator[Message]
  def count: Int
  def sizeBytes: Long
}
