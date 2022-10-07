ThisBuild / version := "0.2.0"
ThisBuild / organization := "com.github.kazanzhy"
ThisBuild / scalaVersion := "2.13.9"

lazy val root = (project in file("."))
    .enablePlugins(AssemblyPlugin, PackPlugin)
    .settings(
      name := "kafka-connect-transform-record2row",
      crossPaths := false,
    )

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "connect-transforms" % "3.3.1" % Provided,
  "org.slf4j" % "slf4j-api" % "2.0.3",
  "at.grahsl.kafka.connect" % "kafka-connect-mongodb" % "1.4.0",
  "org.mongodb" % "bson" % "4.7.1",
  "org.sharegov" % "mjson" % "1.4.1",
  "org.scalatest" %% "scalatest" % "3.2.14" % Test,
)
excludeDependencies ++= Seq(
  ExclusionRule("ch.qos.logback", "logback-classic"),
  ExclusionRule("com.fasterxml.jackson.core", "jackson-core"),
  ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
  ExclusionRule("io.confluent", "kafka-avro-serializer"),
  ExclusionRule("io.confluent", "kafka-connect-maven-plugin"),
  ExclusionRule("org.apache.commons", "commons-lang3"),
  ExclusionRule("org.mongodb", "mongodb-driver"),
  ExclusionRule("junit", "junit")
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
packDir := "kazanzhy-kafka-connect-transform-record2row-0.2.0"

// publish parameters - for publishing to GitHub Maven
credentials += Credentials("GitHub Package Registry", "maven.pkg.github.com", "kazanzhy", System.getenv("GITHUB_TOKEN"))
publishTo := Some("GitHub kazanzhy Apache Maven Packages" at "https://maven.pkg.github.com/kazanzhy/kafka-connect-transform-record2row")
publishMavenStyle := true
publishConfiguration := publishConfiguration.value.withOverwrite(true)
