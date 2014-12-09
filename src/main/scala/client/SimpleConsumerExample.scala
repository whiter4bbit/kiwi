package phi.client

import com.twitter.util.{Duration, Await}
import phi.Message

object SimpleConsumerExample {
  def main(args: Array[String]): Unit = {
    val client = new QueueClient("localhost:8080")

    def consume(): Unit = {
      val future = client.poll("example-topic", 10).map { messages =>
        messages.foreach { m => println(s"${new String(m.payload)}") }
      } 

      try { Await.ready(future, Duration.fromSeconds(30)) } catch { case _ => /* */ }
      consume()
    }

    consume()
  }
}


