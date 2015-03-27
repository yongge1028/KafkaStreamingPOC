name := "KafkaStreamingPOC"

version := "1.0"

//scalaVersion := "2.11.5"
scalaVersion := "2.10.4"

//libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0",
//  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.5.0-cdh5.3.0" % "provided" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
    ),
//  "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0-cdh5.3.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0-cdh5.3.0",
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.0.Beta3",
  "com.maxmind.geoip2" % "geoip2" % "2.1.0")

// If using CDH, also add Cloudera repo
resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

// "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
