package phi.client

import com.twitter.util.Await
import phi.server.AppendMessage

import scala.annotation.tailrec

object ProducerExample {
  def main(args: Array[String]): Unit = {
    val client = new QueueClient("localhost:8080")
    val batchSize = args.toList.headOption.map(_.toInt).getOrElse(10)

    @tailrec def produce(i: Long, buffer: List[AppendMessage]): Unit = {
      val msg = AppendMessage(s"message-$i".getBytes)
      if (buffer.length == batchSize) {
        val result = client.append("example-topic", buffer)
        Await.result(result)
        produce(i + 1, msg::Nil)
      } else {
        produce(i + 1, buffer :+ msg)
      }
    }

    produce(0, Nil)
  }
}
