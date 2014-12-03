import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "persistent-queue"

organization := "phi-org"

version := "0.1"

scalaVersion := "2.10.3"

scalacOptions := Seq("-feature", "-language:implicitConversions", "-language:postfixOps")

initialCommands in Compile := """
|import java.nio.file._
|import phi._""".stripMargin

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-core" % "6.22.0",
  "com.twitter" %% "finagle-http" % "6.22.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
