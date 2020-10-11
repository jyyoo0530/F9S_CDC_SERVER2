name := "F9S_CDC_SERVER"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.0.5",
  "org.mongodb.scala" %% "mongo-scala-bson" % "4.0.5",
  "mysql" % "mysql-connector-java" % "8.0.20",
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
  "io.lettuce" % "lettuce-core" % "5.3.2.RELEASE",
  "org.apache.activemq" % "artemis-server" % "2.14.0",
  "org.apache.activemq" % "artemis-jms-client" % "2.14.0",
  "org.apache.activemq" % "artemis-commons" % "2.14.0",
  "org.apache.activemq" % "artemis-core-client" % "2.14.0",
  "org.apache.activemq" % "artemis-stomp-protocol" % "2.14.0"
)