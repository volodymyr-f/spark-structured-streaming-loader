name := "SparkStructuredStreamingLoader"

version := "0.0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "2.1.0" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-core" % "1.0.0"  withSources() withJavadoc(),
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0"  withSources() withJavadoc(),
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0"  withSources() withJavadoc(),
    "joda-time" % "joda-time" % "2.7",
    "log4j" % "log4j" % "1.2.14",
    "org.apache.spark" %% "spark-sql" % "2.1.0",
    "org.json4s" %% "json4s-native" % "3.2.10"
  )