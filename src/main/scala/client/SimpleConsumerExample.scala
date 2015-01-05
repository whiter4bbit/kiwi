package phi.client

import com.twitter.util.Future

object SimpleConsumerExample {
  def main(args: Array[String]): Unit = {
    val consumer = QueueClient("localhost:8080").simpleConsumer("example-topic", "consumer-1")

    def consume(): Unit = {
      consumer.poll(10).flatMap { messages =>
        messages.foreach(message => println(new String(message.payload)))
        messages.lastOption.map { last =>
          consumer.offset(last.nextOffset)
        }.getOrElse(Future.value(()))
      } ensure consume
    }

    consume()
  }
}
