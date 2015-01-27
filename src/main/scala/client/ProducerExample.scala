package phi.client

import com.twitter.util.{Future, Await, Return, Throw}
import org.jboss.netty.buffer.ChannelBuffers

import scala.annotation.tailrec

import phi.message.Message

object ProducerExample {
  case class Options(batch: Int = 10, producers: Int = 1, size: Int = 128)

  def parse(args: List[String], options: Options): Options = args match {
    case "-batch"::n::tail => parse(tail, options.copy(batch = n.toInt))
    case "-producers"::n::tail => parse(tail, options.copy(producers = n.toInt))
    case "-size"::n::tail => parse(tail, options.copy(size = n.toInt))
    case arg::tail => throw new IllegalArgumentException(s"Unexpected parameter: $arg")
    case Nil => options
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList, Options())

    val producer = QueueClient("localhost:8080").producer("example-topic")

    val message = new Array[Byte](options.size)

    val messages = (0 until options.batch) map { _ =>
      Message(message)
    } toList

    def produce(): Unit = {
      producer.send(messages) ensure {
        produce()
      } respond {
        case Return(_) => /* pass */
        case Throw(th) => th.printStackTrace
      }
    }

    0 until options.producers foreach { _ => produce() }

    while (true) {
      Thread.sleep(10)
    }

  }
}

