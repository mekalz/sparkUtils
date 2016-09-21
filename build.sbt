name := "sparkUtils"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.2"

libraryDependencies <<= scalaVersion {
  scala_version => Seq(
    // Spark and Spark Streaming
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion % "provided"
  )
}