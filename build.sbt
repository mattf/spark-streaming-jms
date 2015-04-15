name := "spark-streaming-jms"

organization := "com.redhat"

version := "0.0.1"

val SPARK_VERSION = "1.3.0"

val LOG4J_VERSION = "1.2.17"

val JMS_VERSION = "1.1-rev-1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"log4j" % "log4j" % LOG4J_VERSION,
"javax.jms" % "jms-api" % JMS_VERSION,
"org.apache.spark" %% "spark-core" % SPARK_VERSION,
"org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
"org.apache.spark" %% "spark-sql" % SPARK_VERSION,
"org.apache.spark" %% "spark-streaming" % SPARK_VERSION
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

fork := true
