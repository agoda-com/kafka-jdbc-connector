package com.agoda.kafka.connector.jdbc

import org.scalatest.{Matchers, WordSpec}

class JdbcSourceConnectorConfigTest extends WordSpec with Matchers {
  import JdbcSourceConnectorConfigTestData._

  "JDBC Source Connector Config" should {

    "should create JDBC source configuration for timestamp mode" in {
      val properties = Map(connectionUrlProperty, incrementingModeProperty, storedProcedureProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, incrementingVariableNameConfig, incrementingFieldNameConfig)

      new JdbcSourceConnectorConfig(properties).getClass shouldEqual classOf[JdbcSourceConnectorConfig]
    }

    "should create JDBC source configuration for incrementing mode" in {
      val properties = Map(connectionUrlProperty, timestampModeProperty, storedProcedureProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, timestampVariableNameConfig, timestampFieldNameConfig)

      new JdbcSourceConnectorConfig(properties).getClass shouldEqual classOf[JdbcSourceConnectorConfig]
    }

    "should create JDBC source configuration for timestamp+incrementing mode" in {
      val properties = Map(connectionUrlProperty, timestampIncrementingModeProperty, storedProcedureProperty,
        topicProperty, pollIntervalProperty, batchVariableNameProperty, timestampVariableNameConfig, timestampFieldNameConfig,
        incrementingVariableNameConfig, incrementingFieldNameConfig)

      new JdbcSourceConnectorConfig(properties).getClass shouldEqual classOf[JdbcSourceConnectorConfig]
    }
  }
}

object JdbcSourceConnectorConfigTestData {
  val connectionUrlProperty: (String, String)             = "connection.url" -> "test-connection"
  val timestampModeProperty: (String, String)             = "mode" -> "timestamp"
  val incrementingModeProperty: (String, String)          = "mode" -> "incrementing"
  val timestampIncrementingModeProperty: (String, String) = "mode" -> "timestamp+incrementing"
  val storedProcedureProperty: (String, String)           = "stored-procedure.name" -> "test-procedure"
  val topicProperty: (String, String)                     = "topic" -> "test-topic"
  val pollIntervalProperty: (String, String)              = "poll.interval.ms" -> "100"
  val batchVariableNameProperty: (String, String)         = "batch.max.rows.variable.name" -> "batch"
  val timestampVariableNameConfig: (String, String)       = "timestamp.variable.name" -> "time"
  val timestampFieldNameConfig: (String, String)          = "timestamp.field.name" -> "time"
  val incrementingVariableNameConfig: (String, String)    = "incrementing.variable.name" -> "id"
  val incrementingFieldNameConfig: (String, String)       = "incrementing.field.name" -> "id"
}
