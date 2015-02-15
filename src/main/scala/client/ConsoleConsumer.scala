package phi.client

import com.twitter.util.{Return, Throw}

object ConsoleConsumer {
  case class Options(address: String = "localhost:5432", topic: String = "console-topic", consumer: Option[String] = None)

  def parse(args: List[String], options: Options = Options()): Option[Options] = {
    def help() = { println(s"Usage: ${getClass} [--address host:port] [--topic topic-name] [--consumer consumer-name]"); None }

    args match {
      case "--address"::address::tail => parse(tail, options.copy(address = address))
      case "--topic"::topic::tail => parse(tail, options.copy(topic = topic))
      case "--consumer"::consumer::tail => parse(tail, options.copy(consumer = Some(consumer)))
      case "--help"::_ => help()
      case Nil => Some(options)
      case unknown => { println(s"Unknown option: ${unknown}"); help() }
    }
  }

  def main(args: Array[String]): Unit = parse(args.toList) match {
    case Some(options) => run(options)
    case None => System.exit(1)
  }

  def run(options: Options): Unit = {
    val client = KiwiClient(options.address)

    options.consumer match {
      case Some(id) => {
        val consumer = client.consumer(options.topic, id)

        def consume(): Unit = {
          consumer.await(1000) { messages =>
            messages.foreach(message => println(new String(message.payload)))
          }.respond {
            case Return(_) => {
              consume()
            }
            case Throw(th) => {
              th.printStackTrace
            }
          }
        }

        consume()
      }

      case None => {
        val consumer = client.consumer(options.topic)

        def consume(): Unit = {
          consumer.await(1000).respond {
            case Return(messages) => {
              messages.foreach(message => println(new String(message.payload)))
              consume()
            }
            case Throw(th) => {
              th.printStackTrace
            }
          }
        }

        consume()
      }
    }

    while (true) { Thread.sleep(1000) }
  }
}
