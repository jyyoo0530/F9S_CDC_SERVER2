scalaVersion := "2.12.10"
lazy val AkkaVersion = "2.6.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "com.typesafe.akka" %% "akka-actor" % AkkaVersion
)