name := "tweetstream"

version := "0.0.1"

scalaVersion := "2.10.4"

sbtVersion := "0.13.5"

fork in run := true

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.twitter4j" % "twitter4j-core" % "4.0.2",
  "org.twitter4j" % "twitter4j-stream" % "4.0.2",
  "com.amazonaws" % "amazon-kinesis-client" % "1.1.0"
)

mainClass in (Compile, run) := Some("jp.co.tis.stc.example.TweetStreamer")

