package phi.client

import com.twitter.util.{Future, Await}
import phi.server.AppendMessage

import scala.annotation.tailrec

object ProducerExample {
  case class Options(batch: Int = 10, producers: Int = 1)

  def parse(args: List[String], options: Options): Options = args match {
    case "-batch"::n::tail => parse(tail, options.copy(batch = n.toInt))
    case "-producers"::n::tail => parse(tail, options.copy(producers = n.toInt))
    case arg::tail => throw new IllegalArgumentException(s"Unexpected parameter: $arg")
    case Nil => options
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args.toList, Options())

    val client = new QueueClient("localhost:8080")

    def produce(i: Long, buffer: List[AppendMessage]): Unit = {
      val msg = AppendMessage(s"message-$i".getBytes)
      if (buffer.length == options.batch) {
        client.append("example-topic", buffer) ensure {
          produce(i + 1, msg::Nil)
        }
      } else {
        produce(i + 1, buffer :+ msg)
      }
    }

    0 until options.producers foreach { _ => produce(0, Nil) }

    while (true) {
      Thread.sleep(10)
    }

  }
}
