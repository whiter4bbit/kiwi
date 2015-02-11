package phi

import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic._
import scala.collection.JavaConversions._

object Sets {
  val set = new ConcurrentSkipListSet[Long]
  val registry = new ConcurrentSkipListSet[Long]
  val random = ThreadLocalRandom.current
  val counter = new AtomicLong(0)
  
  val processor = new Runnable {
    override def run(): Unit = {
      while(true) {
        val iter = set.iterator
        def iterate(): Unit = {
          if (iter.hasNext) {
            val item = iter.next
            if (registry.contains(item)) {
              println(s"duplicated item ${item}")
            }
            if (random.nextInt(0, 10) % 2 == 0) {
              iter.remove
              registry.add(item)
            }
            iterate()
          }
        }
        iterate()
        Thread.sleep(100)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    

    new Thread(processor).start

    (0 until 100).foreach { _ =>
      new Thread {
        override def run(): Unit = {
          while (true) {
            set.add(counter.incrementAndGet)
            if (counter.get % 1000 == 0) println("added 1000 so far")
            Thread.sleep(100)
          }
        }
      }.start
    }
  }
}
