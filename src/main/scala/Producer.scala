package phi

import phi.message.TransferableMessageSet

class Producer(log: Log) {
  def append(payload: Array[Byte]): Unit = 
    log.append(payload)
  def append(set: TransferableMessageSet): Unit = 
    log.append(set)
}

