package phi.client

import com.twitter.util.{Await, Duration}

object AtLeastOnceDeliveryConsumerExample {
  def main(args: Array[String]): Unit = {
    val consumer = QueueClient("localhost:8080").consumer("example-topic", "consumer-1")

    def consume(): Unit = {
      val response = consumer.poll(10)
      val messages = Await.result(response, Duration.fromSeconds(30))
      messages.foreach { message => println(s"${new String(message.payload)}") }
      messages.lastOption.map { message => 
        val offset = message.offset + 4 + message.payload.length
        val commit = consumer.commit(offset)
        Await.result(response, Duration.fromSeconds(10))
      }
      consume()
    }

    consume()
  }
}
