package phi.client

import com.twitter.util.{Duration, Await}

object SimpleConsumerExample {
  def main(args: Array[String]): Unit = {
    val consumer = QueueClient("localhost:8080").consumer("example-topic")

    def consume(): Unit = {
      val future = consumer.poll(10).map { messages =>
        messages.foreach { m => println(s"${new String(m.payload)}") }
      }

      try { Await.ready(future, Duration.fromSeconds(30)) } catch { case _ => /* */ }
      consume()
    }

    consume()
  }
}


