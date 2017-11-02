name := "kafka-eventsource"

version := "0.1"

scalaVersion := "2.12.2"

organization := "org.richarda2b"

val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "io.monix" %% "monix-kafka-10" % "0.14",
  "ch.qos.logback"             %  "logback-classic"          % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging"            % "3.5.0",
  "io.circe"                   %% "circe-core"               % circeVersion,
  "io.circe"                   %% "circe-parser"             % circeVersion,
  "io.circe"                   %% "circe-generic"            % circeVersion,
  "org.scalatest"              %% "scalatest"                % "3.0.3"  % "test"
)

