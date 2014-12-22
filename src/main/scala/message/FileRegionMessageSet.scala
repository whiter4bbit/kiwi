package phi.message

import java.io.File

case class FileRegionMessageSet(offset: Long, length: Long, position: Long, file: File)
