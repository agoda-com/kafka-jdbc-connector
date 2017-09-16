package com.agoda.kafka.connector.jdbc

import com.agoda.kafka.connector.jdbc.models.Mode.TimestampIncrementingMode
import org.scalatest.{Matchers, WordSpec}

class JdbcSourceConnectorConfigTest extends WordSpec with Matchers {
  import JdbcSourceConnectorConfigTestData._

  "JDBC Source Connector Config" should {

    "throw exception if any mandatory configuration is missing" in {
      val properties = Map(connectionUrlProperty, incrementingModeProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, incrementingVariableNameConfig, incrementingFieldNameConfig)

      the [IllegalArgumentException] thrownBy new JdbcSourceConnectorConfig(properties).getClass
    }

    "create JDBC source configuration for incrementing mode" in {
      val properties = Map(connectionUrlProperty, incrementingModeProperty, storedProcedureProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, incrementingVariableNameConfig, incrementingFieldNameConfig)

      new JdbcSourceConnectorConfig(properties).getClass shouldEqual classOf[JdbcSourceConnectorConfig]
    }

    "throw exception if any configuration for incrementing mode is missing" in {
      val properties = Map(connectionUrlProperty, incrementingModeProperty, storedProcedureProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, incrementingVariableNameConfig)

      the [IllegalArgumentException] thrownBy new JdbcSourceConnectorConfig(properties).getClass
    }

    "create JDBC source configuration for timestamp mode" in {
      val properties = Map(connectionUrlProperty, timestampModeProperty, storedProcedureProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, timestampVariableNameConfig, timestampFieldNameConfig)

      new JdbcSourceConnectorConfig(properties).getClass shouldEqual classOf[JdbcSourceConnectorConfig]
    }

    "throw exception if any configuration for timestamp mode is missing" in {
      val properties = Map(connectionUrlProperty, timestampModeProperty, storedProcedureProperty, topicProperty,
        pollIntervalProperty, batchVariableNameProperty, timestampFieldNameConfig)

      the [IllegalArgumentException] thrownBy new JdbcSourceConnectorConfig(properties).getClass
    }

    "create JDBC source configuration for timestamp+incrementing mode" in {
      val properties = Map(connectionUrlProperty, timestampIncrementingModeProperty, storedProcedureProperty,
        topicProperty, pollIntervalProperty, batchVariableNameProperty, timestampVariableNameConfig, timestampFieldNameConfig,
        incrementingVariableNameConfig, incrementingFieldNameConfig)

      new JdbcSourceConnectorConfig(properties).getClass shouldEqual classOf[JdbcSourceConnectorConfig]
    }

    "throw exception if any configuration for timestamp+incrementing mode is missing" in {
      val properties = Map(connectionUrlProperty, timestampIncrementingModeProperty, storedProcedureProperty,
        topicProperty, pollIntervalProperty, batchVariableNameProperty, timestampFieldNameConfig,
        incrementingVariableNameConfig)

      the [IllegalArgumentException] thrownBy new JdbcSourceConnectorConfig(properties).getClass
    }

    "get all properties from configuration" in {
      val properties = Map(connectionUrlProperty, timestampIncrementingModeProperty, storedProcedureProperty,
        topicProperty, pollIntervalProperty, batchSizeProperty, batchVariableNameProperty, timestampVariableNameConfig,
        timestampFieldNameConfig, timestampOffsetConfig, incrementingVariableNameConfig, incrementingFieldNameConfig,
        incrementingOffsetConfig, keyFieldConfig)

      val configuration = new JdbcSourceConnectorConfig(properties)

      configuration.getConnectionUrl shouldBe "test-connection"
      configuration.getMode shouldBe TimestampIncrementingMode
      configuration.getStoredProcedureName shouldBe "test-procedure"
      configuration.getTopic shouldBe "test-topic"
      configuration.getPollInterval shouldBe 100
      configuration.getMaxBatchSize shouldBe 1000
      configuration.getMaxBatchSizeVariableName shouldBe "batch"
      configuration.getTimestampVariableName shouldBe Some("time")
      configuration.getTimestampFieldName shouldBe Some("time")
      configuration.getIncrementingVariableName shouldBe Some("id")
      configuration.getIncrementingFieldName shouldBe Some("id")
      configuration.getTimestampOffset shouldBe 946659600000L
      configuration.getIncrementingOffset shouldBe 5L
      configuration.getKeyField shouldBe Some("test-key")
    }

    "get optional properties with default values from configuration" in {
      val properties = Map(connectionUrlProperty, timestampIncrementingModeProperty, storedProcedureProperty,
        topicProperty, batchVariableNameProperty, timestampVariableNameConfig, timestampFieldNameConfig,
        incrementingVariableNameConfig, incrementingFieldNameConfig)

      val configuration = new JdbcSourceConnectorConfig(properties)

      configuration.getPollInterval shouldBe 5000
      configuration.getMaxBatchSize shouldBe 100
      configuration.getTimestampOffset shouldBe 0L
      configuration.getIncrementingOffset shouldBe 0L
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
  val batchSizeProperty: (String, String)                 = "batch.max.records" -> "1000"
  val batchVariableNameProperty: (String, String)         = "batch.max.rows.variable.name" -> "batch"
  val timestampVariableNameConfig: (String, String)       = "timestamp.variable.name" -> "time"
  val timestampFieldNameConfig: (String, String)          = "timestamp.field.name" -> "time"
  val timestampOffsetConfig: (String, String)             = "timestamp.offset" -> "2000-01-01 00:00:00"
  val incrementingVariableNameConfig: (String, String)    = "incrementing.variable.name" -> "id"
  val incrementingFieldNameConfig: (String, String)       = "incrementing.field.name" -> "id"
  val incrementingOffsetConfig: (String, String)          = "incrementing.offset" -> "5"
  val keyFieldConfig: (String, String)                    = "key.field.name" -> "test-key"
}
