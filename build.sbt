name := "kafka-bug"

version := "0.1"

scalaVersion := "2.12.9"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.2.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.2.0"
)