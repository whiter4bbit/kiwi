package phi.client

import com.twitter.util.{Await, Try, Return, Throw}
import jline.console.ConsoleReader

object ConsoleProducer {
  case class Options(address: String = "localhost:5432", topic: String = "console-topic")

  def parse(args: List[String], options: Options = Options()): Option[Options] = {
    def help() = { println(s"Usage: ${getClass} [--address host:port] [--topic topic-name]"); None }

    args match {
      case "--address"::address::tail => parse(tail, options.copy(address = address))
      case "--topic"::topic::tail => parse(tail, options.copy(topic = topic))
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
    val console = new ConsoleReader
    console.setPrompt("producer > ")

    val producer = KiwiClient(options.address).producer(options.topic)

    def read(): Unit = {
      val line = console.readLine
      if (line != null) {
        val result = Try(Await.result(producer.send(line.getBytes)))
        result match {
          case Return(_) => {
            console.println("OK")
            read()
          }
          case Throw(th) => {
            console.println(th.toString)
            read()
          }
        }
      }
    }

    read()
  }
}
