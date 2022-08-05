ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.github.kazanzhy"
ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
    .enablePlugins(AssemblyPlugin, PackPlugin)
    .settings(
      name := "kafka-connect-transform-record2row",
      crossPaths := false,
    )

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "7.2.1-ccs",
  "org.apache.kafka" % "connect-api" % "3.2.1",
  "org.apache.kafka" % "connect-transforms" % "3.2.1",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "at.grahsl.kafka.connect" % "kafka-connect-mongodb" % "1.4.0",
  "org.mongodb" % "mongodb-driver" % "3.12.11",
  "org.mongodb" % "bson" % "4.6.0",
  "io.confluent" % "kafka-avro-serializer" % "7.2.1",
  "io.confluent" % "kafka-connect-maven-plugin" % "0.12.0",
  "ch.qos.logback" % "logback-core" % "1.2.11",
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  "org.scalatest" %% "scalatest" % "3.2.12" % Test,
)

// assembly plugin parameters - for uber JAR
assemblyJarName := "kafka-connect-transform-record2row-assembly.jar"
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.last
}

// pack plugin parameters - for Confluent plugin
packGenerateWindowsBatFile := false
packGenerateMakefile := false
packDir := "kafka-connect-transform-record2row"

// publish parameters - for publishing to GitHub Maven
credentials += Credentials("GitHub Package Registry", "maven.pkg.github.com", "kazanzhy", System.getenv("GITHUB_TOKEN"))
publishTo := Some("GitHub kazanzhy Apache Maven Packages" at "https://maven.pkg.github.com/kazanzhy/kafka-connect-transform-record2row")
publishMavenStyle := true
publishConfiguration := publishConfiguration.value.withOverwrite(true)
