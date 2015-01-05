package phi.message

import phi.LogFileRegion

case class FileChannelMessagesPointer(region: LogFileRegion, offset: Long, count: Long)

