name := "spark-streaming-jms"

organization := "com.redhat"

version := "0.0.1"

val SPARK_VERSION = "1.3.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % SPARK_VERSION,
"org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
"org.apache.spark" %% "spark-sql" % SPARK_VERSION,
"org.apache.spark" %% "spark-streaming" % SPARK_VERSION
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

fork := true
