import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "persistent-queue"

organization := "phi-org"

version := "0.1"

scalaVersion := "2.11.5"

scalacOptions := Seq("-feature", "-language:implicitConversions", "-language:postfixOps")

resolvers += "twttr" at "http://maven.twttr.com/"

initialCommands in Compile := """
|import java.nio.file._
|import phi._""".stripMargin

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-stats" % "6.24.0",
  "com.twitter" %% "twitter-server" % "1.9.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
