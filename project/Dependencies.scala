import sbt._

object Dependencies {
  private val ScalaTestV = "3.0.1"

  private val LogBack             = "ch.qos.logback"              % "logback-classic" % "1.2.3"
  private val ScalaLogging        = "com.typesafe.scala-logging" %% "scala-logging"   % "3.5.0"
  private val KafkaConnectApi     = "org.apache.kafka"            % "connect-api"     % "0.9.0.0"
  private val Enumeratum          = "com.beachape"               %% "enumeratum"      % "1.5.12"
  private val Scalatics           = "org.scalactic"              %% "scalactic"       % ScalaTestV  % "test"
  private val ScalaTest           = "org.scalatest"              %% "scalatest"       % ScalaTestV  % "test"
  private val Mockito             = "org.mockito"                 % "mockito-core"    % "2.10.0"     % "test"

  object Compile {
    def kafkaJdbcConnector  = Seq(LogBack, ScalaLogging, KafkaConnectApi, Enumeratum)
  }

  object Test {
    def kafkaJdbcConnector = Seq(LogBack, ScalaLogging, Scalatics, ScalaTest, Mockito)
  }
}