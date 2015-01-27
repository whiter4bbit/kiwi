package phi.message

import java.nio.channels.FileChannel

trait MessageBatchWithOffset extends MessageBatch {
  def offset: Long
}

object MessageBatchWithOffset {
  def apply(startOffset: Long, messageBatch: MessageBatch) = 
    new MessageBatchWithOffset {
      def logFileRegion = messageBatch.logFileRegion
      def channelBuffer = messageBatch.channelBuffer
      def transferTo(ch: FileChannel) = messageBatch.transferTo(ch)
      def iterator = messageBatch.iterator
      def count = messageBatch.count
      def sizeBytes = messageBatch.sizeBytes
      def offset = startOffset + sizeBytes
    }
}

