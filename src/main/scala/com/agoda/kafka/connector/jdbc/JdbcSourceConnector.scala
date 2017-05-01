package com.agoda.kafka.connector.jdbc

import java.util

import com.agoda.kafka.connector.jdbc.utils.Version
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class JdbcSourceConnector extends SourceConnector {
  private val logger = LoggerFactory.getLogger(classOf[JdbcSourceConnector])

  private var config: JdbcSourceConnectorConfig = _

  override def version(): String = Version.getVersion

  override def start(props: util.Map[String, String]): Unit = {
    Try (new JdbcSourceConnectorConfig(props.asScala.toMap)) match {
      case Success(c) => config = c
      case Failure(e) => throw new ConnectException("Couldn't start com.agoda.kafka.connector.jdbc.JdbcSourceConnector due to configuration error", e)
    }
  }

  override def stop(): Unit = {
    logger.debug("Stopping kafka source connector")
  }

  override def taskClass(): Class[_ <: Task] = classOf[JdbcSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = List(config.properties.asJava).asJava
}