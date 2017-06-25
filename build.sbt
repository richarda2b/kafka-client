name := "kafka-eventsource"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "io.monix" %% "monix-kafka-10" % "0.14",
  "ch.qos.logback"             %  "logback-classic"          % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging"            % "3.5.0",
  "org.scalatest"              %% "scalatest"                % "3.0.3"  % "test"
)

