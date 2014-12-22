package phi

import java.nio.channels.FileChannel

case class LogFileRegion(channel: FileChannel, position: Long, count: Long)
