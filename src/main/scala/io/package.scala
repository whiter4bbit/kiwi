package phi

import java.io.{File => JFile}
import java.nio.file.{Path => JPath, Paths => JPaths}

package object io {
  implicit def jpath2PhiPath(path: JPath) = PhiPath(path)
  implicit def jfile2PhiPath(file: JFile) = PhiPath(file.toPath)
  implicit def string2PhiPath(seg: String) = PhiPath(JPaths.get(seg))
  implicit def phiPath2JPath(phiPath: PhiPath) = phiPath.path
}
