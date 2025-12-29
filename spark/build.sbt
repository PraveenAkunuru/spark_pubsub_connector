name := "spark-pubsub-connector"
version := "0.1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.apache.spark" %% "spark-catalyst" % "3.5.3",
  "org.apache.arrow" % "arrow-vector" % "15.0.2",
  "org.apache.arrow" % "arrow-memory-netty" % "15.0.2",
  "org.apache.arrow" % "arrow-c-data" % "15.0.2",
  "com.google.cloud" % "google-cloud-pubsub" % "1.127.1",
  "com.google.cloud" % "google-cloud-core" % "2.33.0",
  "com.google.auth" % "google-auth-library-oauth2-http" % "1.23.0"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
)

import sbtassembly.AssemblyPlugin.autoImport._

assembly / assemblyMergeStrategy := {
  case PathList("org", "apache", "commons", "logging", _*) => MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case "arrow-git.properties" => MergeStrategy.discard
  case "mozilla/public-suffix-list.txt" => MergeStrategy.first
  case "module-info.class" => MergeStrategy.discard
  case PathList("META-INF", "versions", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
