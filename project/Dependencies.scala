import sbt._

object Dependencies {
  private val KafkaV         = "0.9.0.0"
  private val ScalaTestV     = "3.0.1"

  private val ScalaLogging     = "com.typesafe.scala-logging" %% "scala-logging"        % "3.5.0"
  private val MysqlDriver      = "mysql"                       % "mysql-connector-java" % "6.0.6"
  private val MssqlDriver      = "com.microsoft.sqlserver"     % "mssql-jdbc"           % "6.1.0.jre8" exclude("javax.xml.stream", "stax-api")
  private val SqliteDriver     = "org.xerial"                  % "sqlite-jdbc"          % "3.16.1"
  private val PostgresqlDriver = "org.postgresql"              % "postgresql"           % "42.0.0"
  private val KafkaConnectApi  = "org.apache.kafka"            % "connect-api"          % KafkaV
  private val Scalatics        = "org.scalactic"              %% "scalactic"            % ScalaTestV   % "test"
  private val ScalaTest        = "org.scalatest"              %% "scalatest"            % ScalaTestV   % "test"

  object Compile {
    def kafkaJdbcConnector  = Seq(ScalaLogging, MysqlDriver, MssqlDriver, SqliteDriver, PostgresqlDriver, KafkaConnectApi)
  }

  object Test {
    def kafkaJdbcConnector = Seq(Scalatics, ScalaTest)
  }
}
