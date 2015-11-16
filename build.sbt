organization  := "com.softwaremill"

name := "streams-tests"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.0"

libraryDependencies ++= Seq(
  // akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0-M1",
  // scalaz
  "org.scalaz.stream" %% "scalaz-stream" % "0.8",
  // util
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "joda-time" % "joda-time" % "2.8.2",
  "org.joda" % "joda-convert" % "1.7",
  "org.scalacheck" %% "scalacheck" % "1.12.5"
)
