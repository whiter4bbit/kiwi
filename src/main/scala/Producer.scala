package phi

import phi.bytes._

class Producer(log: Log) {
  def append(chunk: ByteChunk): Unit = 
    log.append(chunk)
}

