package phi

import java.io.File

class LogSegment(file: File) {
  def read(offset: LogOffset): LogView = {
    new LogView(file, offset)
  }
}
