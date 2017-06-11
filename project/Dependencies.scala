import sbt._

object Dependencies {
  private val KafkaV     = "0.9.0.0"
  private val ScalaTestV = "3.0.1"

  private val ScalaLogging        = "com.typesafe.scala-logging" %% "scala-logging"        % "3.5.0"
  private val MysqlDriver         = "mysql"                       % "mysql-connector-java" % "6.0.6"
  private val MssqlDriver         = "com.microsoft.sqlserver"     % "mssql-jdbc"           % "6.1.0.jre8" exclude("javax.xml.stream", "stax-api")
  private val PostgresqlDriver    = "org.postgresql"              % "postgresql"           % "42.0.0"
  private val KafkaConnectApi     = "org.apache.kafka"            % "connect-api"          % KafkaV
  private val Enumeratum          = "com.beachape"               %% "enumeratum"           % "1.5.12"
  private val Scalatics           = "org.scalactic"              %% "scalactic"            % ScalaTestV  % "test"
  private val ScalaTest           = "org.scalatest"              %% "scalatest"            % ScalaTestV  % "test"
  private val Mockito             = "org.mockito"                 % "mockito-core"         % "2.8.9"     % "test"

  object Compile {
    def kafkaJdbcConnector  = Seq(
      ScalaLogging, KafkaConnectApi, MysqlDriver, MssqlDriver, PostgresqlDriver, Enumeratum
    )
  }

  object Test {
    def kafkaJdbcConnector = Seq(Scalatics, ScalaTest, Mockito)
  }
}