package phi.message

import java.nio.channels.FileChannel
import scala.annotation.tailrec

trait LazyMessage {
  def getOffset: Long
  def getLength: Int
  def getPayload: Array[Byte]
  def getPosition: Long
}

class MessageReader(private val newIterator: () => MessageIterator) {
  def foldLeft[A](zero: A)(f: (A, LazyMessage) => A): A = {
    val iter = newIterator()

    val message = new LazyMessage {
      def getOffset = iter.getOffset
      def getLength = iter.getLength
      def getPayload = iter.getPayload
      def getPosition = iter.getPosition
    }

    @tailrec def fold(zero: A)(f: (A, LazyMessage) => A): A = if (iter.next) {
      fold(f(zero, message))(f)
    } else zero

    fold(zero)(f)
  }

  class LimitingIterator(max: Int) extends MessageIterator {
    val underlying = newIterator()
    var count = 0

    def next: Boolean = if (count < max) {
      count += 1
      underlying.next
    } else false
    def getOffset = underlying.getOffset
    def getLength = underlying.getLength
    def getPayload = underlying.getPayload
    def getPosition = underlying.getPosition
  }

  def limit(n: Int): MessageReader = {
    new MessageReader(() => new LimitingIterator(n))
  }

  def count(): Int = {
    val iter = newIterator()
    def c(n: Int): Int = if (iter.next) {
      c(n + 1) 
    } else n
    c(0)
  }
}

object MessageReader {
  def apply(channel: FileChannel, position: Long) = {
    new MessageReader(() => new FileChannelIterator(channel, position))
  }
}
