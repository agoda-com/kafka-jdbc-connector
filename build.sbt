import sbt._
import sbt.Keys._

lazy val `kafka-jdbc-connector` =
  (project in file("."))
    .settings(
      name := "kafka-jdbc-connector",
      version := "0.9.0.0",
      organization := "com.agoda",
      scalaVersion := "2.11.7",
      crossScalaVersions := Seq("2.11.7", "2.12.2"),
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