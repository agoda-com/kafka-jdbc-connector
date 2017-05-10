package com.agoda.kafka.connector.jdbc

import java.sql.Timestamp

import com.agoda.kafka.connector.jdbc.JdbcSourceConnectorConstants._
import com.agoda.kafka.connector.jdbc.models.Mode
import com.agoda.kafka.connector.jdbc.models.Mode.{IncrementingMode, TimestampIncrementingMode, TimestampMode}

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

  def getConnectionUrl: String = properties(CONNECTION_URL_CONFIG)

  def getMode: Mode = Mode.withName(properties(MODE_CONFIG))

  def getStoredProcedureName: String = properties(STORED_PROCEDURE_NAME_CONFIG)

  def getTopic: String = properties(TOPIC_CONFIG)

  def getPollInterval: Long = properties.getOrElse(POLL_INTERVAL_MS_CONFIG, POLL_INTERVAL_MS_DEFAULT).toLong

  def getMaxBatchSize: Int = properties.getOrElse(BATCH_MAX_ROWS_CONFIG, BATCH_MAX_ROWS_DEFAULT).toInt

  def getMaxBatchSizeVariableName: String = properties(BATCH_MAX_ROWS_VARIABLE_NAME_CONFIG)

  def getTimestampVariableName: Option[String] = properties.get(TIMESTAMP_VARIABLE_NAME_CONFIG)

  def getTimestampFieldName: Option[String] = properties.get(TIMESTAMP_FIELD_NAME_CONFIG)

  def getIncrementingVariableName: Option[String] = properties.get(INCREMENTING_VARIABLE_NAME_CONFIG)

  def getIncrementingFieldName: Option[String] = properties.get(INCREMENTING_FIELD_NAME_CONFIG)

  def getTimestampOffset: Long = {
    val maybeTimestamp = properties.get(TIMESTAMP_OFFSET_CONFIG).map(o => Timestamp.valueOf(o))
    maybeTimestamp.getOrElse(TIMESTAMP_OFFSET_DEFAULT).getTime
  }

  def getIncrementingOffset: Long = properties.getOrElse(INCREMENTING_OFFSET_CONFIG, INCREMENTING_OFFSET_DEFAULT).toLong

  def getKeyField: Option[String] = properties.get(KEY_FIELD_NAME_CONFIG)
}