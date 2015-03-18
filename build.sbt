import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "kiwi"

organization := "phi"

version := "0.1"

scalaVersion := "2.11.5"

scalacOptions := Seq("-feature", "-language:implicitConversions", "-language:postfixOps", "-Xlint")

assemblyJarName in assembly := "kiwi.jar"

mainClass := Some("phi.server.KiwiServer")

resolvers += "twttr" at "http://maven.twttr.com/"

initialCommands in Compile := """
|import java.nio.file._
|import phi._""".stripMargin

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-stats" % "6.24.0",
  "com.twitter" %% "finagle-http" % "6.24.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.typesafe" % "config" % "1.2.1",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "org.slf4j" % "jul-to-slf4j" % "1.7.10",
  "jline" % "jline" % "2.12.1",
  "org.json4s" %% "json4s-native" % "3.2.10"
)
