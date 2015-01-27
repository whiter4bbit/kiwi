package phi

import phi.message.MessageBatch

class Producer(log: Log) {
  def append(batch: MessageBatch): Unit = 
    log.append(batch)
}

