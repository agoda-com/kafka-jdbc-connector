package com.agoda.kafka.connector.jdbc

import java.sql.Timestamp

object JdbcSourceConnectorConstants {

  val STORED_PROCEDURE_NAME_KEY           = "stored-procedure.name"

  val CONNECTION_URL_CONFIG               = "connection.url"

  val MODE_CONFIG                         = "mode"
  val TIMESTAMP_VARIABLE_NAME_CONFIG      = "timestamp.variable.name"
  val TIMESTAMP_FIELD_NAME_CONFIG         = "timestamp.field.name"
  val INCREMENTING_VARIABLE_NAME_CONFIG   = "incrementing.variable.name"
  val INCREMENTING_FIELD_NAME_CONFIG      = "incrementing.field.name"

  val STORED_PROCEDURE_NAME_CONFIG        = "stored-procedure.name"

  val TOPIC_CONFIG                        = "topic"

  val POLL_INTERVAL_MS_CONFIG             = "poll.interval.ms"
  val POLL_INTERVAL_MS_DEFAULT            = "5000"

  val BATCH_MAX_ROWS_VARIABLE_NAME_CONFIG = "batch.max.rows.variable.name"
  val BATCH_MAX_ROWS_CONFIG               = "batch.max.records"
  val BATCH_MAX_ROWS_DEFAULT              = "100"

  val TIMESTAMP_OFFSET_CONFIG             = "timestamp.offset"
  val TIMESTAMP_OFFSET_DEFAULT            = new Timestamp(0L)
  val INCREMENTING_OFFSET_CONFIG          = "incrementing.offset"
  val INCREMENTING_OFFSET_DEFAULT         = "0"

  val KEY_FIELD_NAME_CONFIG               = "key.field.name"

  val TASKS_MAX_CONFIG                    = "tasks.max"
  val CONNECTOR_CLASS                     = "connector.class"
}
