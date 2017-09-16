package com.agoda.kafka.connector.jdbc

import java.sql.Timestamp
import java.util.TimeZone

import com.agoda.kafka.connector.jdbc.JdbcSourceConnectorConstants._
import com.agoda.kafka.connector.jdbc.models.Mode
import com.agoda.kafka.connector.jdbc.models.Mode.{IncrementingMode, TimestampIncrementingMode, TimestampMode}

/**
  * @constructor
  * @param properties is set of configurations required to create JdbcSourceConnectorConfig
  */
class JdbcSourceConnectorConfig(val properties: Map[String, String]) {

  require(
    properties.contains(CONNECTION_URL_CONFIG) &&
    properties.contains(MODE_CONFIG) &&
    properties.contains(STORED_PROCEDURE_NAME_CONFIG) &&
    properties.contains(TOPIC_CONFIG) &&
    properties.contains(BATCH_MAX_ROWS_VARIABLE_NAME_CONFIG),
    s"""Required connector properties:
       |  $CONNECTION_URL_CONFIG
       |  $MODE_CONFIG
       |  $STORED_PROCEDURE_NAME_CONFIG
       |  $TOPIC_CONFIG
       |  $BATCH_MAX_ROWS_VARIABLE_NAME_CONFIG""".stripMargin
  )

  require(
    Mode.withName(properties(MODE_CONFIG)) match {
      case TimestampMode              => properties.contains(TIMESTAMP_VARIABLE_NAME_CONFIG) &&
                                         properties.contains(TIMESTAMP_FIELD_NAME_CONFIG)
      case IncrementingMode           => properties.contains(INCREMENTING_VARIABLE_NAME_CONFIG) &&
                                         properties.contains(INCREMENTING_FIELD_NAME_CONFIG)
      case TimestampIncrementingMode  => properties.contains(TIMESTAMP_VARIABLE_NAME_CONFIG) &&
                                         properties.contains(TIMESTAMP_FIELD_NAME_CONFIG) &&
                                         properties.contains(INCREMENTING_VARIABLE_NAME_CONFIG) &&
                                         properties.contains(INCREMENTING_FIELD_NAME_CONFIG)
    },
    Mode.withName(properties(MODE_CONFIG)) match {
      case TimestampMode              => s"""Required connector properties:
                                             |  $TIMESTAMP_VARIABLE_NAME_CONFIG
                                             |  $TIMESTAMP_FIELD_NAME_CONFIG""".stripMargin
      case IncrementingMode           => s"""Required connector properties:
                                             |  $INCREMENTING_VARIABLE_NAME_CONFIG
                                             |  $INCREMENTING_FIELD_NAME_CONFIG""".stripMargin
      case TimestampIncrementingMode  => s"""Required connector properties:
                                             |  $TIMESTAMP_VARIABLE_NAME_CONFIG
                                             |  $TIMESTAMP_FIELD_NAME_CONFIG
                                             |  $INCREMENTING_VARIABLE_NAME_CONFIG
                                             |  $INCREMENTING_FIELD_NAME_CONFIG""".stripMargin
    }
  )

/**
  * @return database connection url
  */
  def getConnectionUrl: String = properties(CONNECTION_URL_CONFIG)

/**
  * @return mode of operation [[IncrementingMode]], [[TimestampMode]], [[TimestampIncrementingMode]]
  */
  def getMode: Mode = Mode.withName(properties(MODE_CONFIG))

/**
  * @return stored procedure name
  */
  def getStoredProcedureName: String = properties(STORED_PROCEDURE_NAME_CONFIG)

/**
  * @return kafka topic name
  */
  def getTopic: String = properties(TOPIC_CONFIG)

/**
  * @return database poll interval
  */
  def getPollInterval: Long = properties.getOrElse(POLL_INTERVAL_MS_CONFIG, POLL_INTERVAL_MS_DEFAULT).toLong

/**
  * @return number of records fetched in each poll
  */
  def getMaxBatchSize: Int = properties.getOrElse(BATCH_MAX_ROWS_CONFIG, BATCH_MAX_ROWS_DEFAULT).toInt

/**
  * @return batch size variable name in stored procedure
  */
  def getMaxBatchSizeVariableName: String = properties(BATCH_MAX_ROWS_VARIABLE_NAME_CONFIG)

/**
  * @return timestamp offset variable name in stored procedure
  */
  def getTimestampVariableName: Option[String] = properties.get(TIMESTAMP_VARIABLE_NAME_CONFIG)

/**
  * @return timestamp offset field name in record
  */
  def getTimestampFieldName: Option[String] = properties.get(TIMESTAMP_FIELD_NAME_CONFIG)

/**
  * @return incrementing offset variable name in stored procedure
  */
  def getIncrementingVariableName: Option[String] = properties.get(INCREMENTING_VARIABLE_NAME_CONFIG)

/**
  * @return incrementing offset field name in record
  */
  def getIncrementingFieldName: Option[String] = properties.get(INCREMENTING_FIELD_NAME_CONFIG)

/**
  * @return initial timestamp offset
  */
  def getTimestampOffset: Long = {
    properties
      .get(TIMESTAMP_OFFSET_CONFIG)
      .map(o => new Timestamp(Timestamp.valueOf(o).getTime + TimeZone.getDefault.getRawOffset))
      .getOrElse(TIMESTAMP_OFFSET_DEFAULT).getTime
  }

/**
  * @return initial incrementing offset
  */
  def getIncrementingOffset: Long = properties.getOrElse(INCREMENTING_OFFSET_CONFIG, INCREMENTING_OFFSET_DEFAULT).toLong

/**
  * @return optional field name to be used as kafka message key
  */
  def getKeyField: Option[String] = properties.get(KEY_FIELD_NAME_CONFIG)
}