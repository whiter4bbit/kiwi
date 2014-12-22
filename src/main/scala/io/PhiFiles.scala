package phi.io

import java.nio.file.{Path => JPath, Paths => JPaths, Files => JFiles}
import java.io.{File => JFile}

object PhiFiles {

  def tempDirectory: JPath = JPaths.get(System.getProperty("java.io.tmpdir"))

  def withTempDir[A](prefix: String)(f: JPath => A): A = {
    val tempDir = JFiles.createTempDirectory(tempDirectory, prefix)
    try f(tempDir) finally tempDir.deleteDirectory
  }

  def createTempFile(prefix: String, suffix: String, deleteOnExit: Boolean = false): PhiPath = {
    val file = JFile.createTempFile(prefix, suffix)
    if (deleteOnExit) file.deleteOnExit()
    file
  }

}
