organization  := "com.softwaremill"

name := "streams-tests"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.6"

val akkaVersion = "2.3.10"

libraryDependencies ++= Seq(
  // akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC2",
  // scalaz
  "org.scalaz.stream" %% "scalaz-stream" % "0.7a",
  // util
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "joda-time" % "joda-time" % "2.7",
  "org.joda" % "joda-convert" % "1.7",
  "org.scalacheck" %% "scalacheck" % "1.12.3"
)
