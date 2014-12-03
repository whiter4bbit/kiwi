package phi

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

class Topics(baseDir: Path) {
  private val topics = new ConcurrentHashMap[String, Topic]

  case class Topic(_log: () => Log, _offset: () => LogOffset) {
    private val log = _log()
    private val offset = _offset()
    private val _reader = new AtomicLogReader(log, offset)

    def append(payload: Array[Byte]): Unit =
      log.append(payload)

    def view(): LogReader = 
      _reader
  }

  def getTopic(name: String) = {
    var topic = topics.get(name)
    if (topic == null) {
      topic = Topic(() => new Log(baseDir, name), () => new LogOffset(baseDir, name))
      val prev = topics.putIfAbsent(name, topic)
      if (prev != null) {
        topic = prev
      }
    }
    topic
  }
}
