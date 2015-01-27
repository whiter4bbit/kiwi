package phi

import phi.message2.MessageBatch

class Producer(log: Log) {
  def append(batch: MessageBatch): Unit = 
    log.append(batch)
}

