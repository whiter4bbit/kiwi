package phi.client

import com.twitter.util.{Duration, Await}

object GlobalConsumerExample {
  def main(args: Array[String]): Unit = {
    val consumer = QueueClient("localhost:8080").consumer("example-topic")

    def consume(): Unit = {
      consumer.poll(10).map { messages =>
        messages.foreach { m => println(new String(m.payload)) }
      } ensure consume
    }

    consume()
  }
}


