package phi

import java.nio.channels.FileChannel
import java.nio.ByteBuffer

import scala.annotation.tailrec

import phi.message.{MessageAndOffset, FileChannelMessagesPointer, MessageReader, MessageIterator}

class LogSegmentView(channel: FileChannel, offset: Long, max: Int) {
  def pointer: FileChannelMessagesPointer = {
    val reader = MessageReader(channel, offset).limit(max)
    val (count, lastOffset) = reader.foldLeft((0, offset)) { (countAndOffset, message) =>
      (countAndOffset._1 + 1, message.getPosition)
    }

    FileChannelMessagesPointer(LogFileRegion(channel, offset, lastOffset - offset), offset, count)
  }

  def messages: List[MessageAndOffset] = {
    MessageReader(channel, offset).limit(max).foldLeft(List.empty[MessageAndOffset]) { (list, message) => 
      list :+ MessageAndOffset(message.getOffset, message.getPosition, message.getPayload)
    }
  }
}
