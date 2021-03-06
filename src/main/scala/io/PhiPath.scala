package phi.io

import java.nio.file.{Path => JPath, Paths => JPaths,Files => JFiles, FileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes
import java.io.{File => JFile, FileFilter => JFileFilter, RandomAccessFile, IOException}

case class PhiPath(path: JPath) {
  def exists: Boolean = JFiles.exists(path)
  def createDirectories(): JPath = JFiles.createDirectories(path)
  def delete(): Unit = JFiles.delete(path)
  def walkFileTree(visitor: FileVisitor[JPath]): JPath = {
    JFiles.walkFileTree(path, visitor)
    path
  }
  def walkFileTree(_postVisitDirectory: (JPath, IOException) => FileVisitResult, 
    _preVisitDirectory: (JPath, BasicFileAttributes) => FileVisitResult,
    _visitFile: (JPath, BasicFileAttributes) => FileVisitResult,
    _visitFileFailed: (JPath, IOException) => FileVisitResult): JPath = {
    val visitor = new FileVisitor[JPath] {
      def postVisitDirectory(path: JPath, e: IOException) = _postVisitDirectory(path, e)
      def preVisitDirectory(path: JPath, attributes: BasicFileAttributes) = _preVisitDirectory(path, attributes)
      def visitFile(path: JPath, attributes: BasicFileAttributes) = _visitFile(path, attributes)
      def visitFileFailed(path: JPath, e: IOException) = _visitFileFailed(path, e)
    }
    walkFileTree(visitor)
  }
  def deleteDirectory(): Unit = {
    import FileVisitResult._
    def postVisitDirectory(path: JPath, e: IOException) = if (e == null) {
      JFiles.delete(path)
      CONTINUE
    } else throw e
    def visitFile(path: JPath, attributes: BasicFileAttributes) = {
      JFiles.delete(path)
      CONTINUE
    }
    walkFileTree(postVisitDirectory, (_, _) => CONTINUE, visitFile, (_, _) => CONTINUE)
  }
  def newRandomAccessFile(mode: String) = {
    new RandomAccessFile(path.toFile, mode)
  }
  def readAllBytes(): Array[Byte] = {
    JFiles.readAllBytes(path)
  }
  def write(bytes: Array[Byte]): Unit = {
    JFiles.write(path, bytes)
  }
  def resolve(seg: String) = {
    PhiPath(path.resolve(seg))
  }
  def /(seg: String) = {
    resolve(seg)
  }
  def toFile() = {
    path.toFile
  }
  def listFiles(f: (JFile => Boolean)): List[PhiPath] = {
    toFile.listFiles(new JFileFilter() {
      def accept(file: JFile) = f(file)
    }).map(file => PhiPath(file.toPath)).toList
  }
  def listFiles(): List[PhiPath] = {
    listFiles(_ => true)
  }
}
