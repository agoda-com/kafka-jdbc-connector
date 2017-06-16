package com.agoda.kafka.connector.jdbc

/**
  * @constructor
  * @param properties is set of configurations required to create JdbcSourceTaskConfig
  */
class JdbcSourceTaskConfig(properties: Map[String, String]) extends JdbcSourceConnectorConfig(properties)