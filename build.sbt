import sbt._
import sbt.Keys._

lazy val commonSettings = Seq(
  version := "0.9.0.0",
  organization := "com.agoda",
  scalaVersion := "2.12.1"
)

lazy val `kafka-jdbc-connector` =
  (project in file("."))
    .settings(
      name := "kafka-jdbc-connector",
      commonSettings,
      libraryDependencies ++= Dependencies.Compile.kafkaJdbcConnector ++ Dependencies.Test.kafkaJdbcConnector
    )
    .enablePlugins(BuildInfoPlugin)
    .settings(
      buildInfoKeys := Seq[BuildInfoKey](version),
      buildInfoPackage := organization.value
    )
    .settings(
      test in assembly := {},
      assemblyJarName in assembly := s"kafka-jdbc-connector-${version.value}.jar"
    )