import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "kiwi"

organization := "phi"

version := "0.1"

scalaVersion := "2.11.5"

scalacOptions := Seq("-feature", "-language:implicitConversions", "-language:postfixOps", "-Xlint")

resolvers += "twttr" at "http://maven.twttr.com/"

initialCommands in Compile := """
|import java.nio.file._
|import phi._""".stripMargin

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-stats" % "6.24.1",
  "com.twitter" %% "twitter-server" % "1.9.0" exclude("com.twitter", "finagle-core"),
  "com.twitter" %% "finagle-core" % "6.24.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.typesafe" % "config" % "1.2.1"
)
