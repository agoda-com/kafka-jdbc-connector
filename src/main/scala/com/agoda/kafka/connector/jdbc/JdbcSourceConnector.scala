package com.agoda.kafka.connector.jdbc

import java.util

import com.agoda.kafka.connector.jdbc.utils.Version
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceConnector, SourceTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class JdbcSourceConnector extends SourceConnector {
  private val logger = LoggerFactory.getLogger(classOf[JdbcSourceConnector])

  private var config: JdbcSourceConnectorConfig = _

/**
  * @return version of this connector
  */
  override def version: String = Version.getVersion

/**
  * invoked by kafka-connect runtime to start this connector
  *
  * @param props properties required to start this connector
  */
  override def start(props: util.Map[String, String]): Unit = {
    Try (new JdbcSourceConnectorConfig(props.asScala.toMap)) match {
      case Success(c) => config = c
      case Failure(e) => logger.error("Couldn't start com.agoda.kafka.connector.jdbc.JdbcSourceConnector due to configuration error", new ConnectException(e))
    }
  }

/**
  * invoked by kafka-connect runtime to stop this connector
  */
  override def stop(): Unit = {
    logger.debug("Stopping kafka source connector")
  }

/**
  * invoked by kafka-connect runtime to instantiate SourceTask which polls data from external data store and saves into kafka
  *
  * @return class of source task to be created
  */
  override def taskClass(): Class[_ <: SourceTask] = classOf[JdbcSourceTask]

/**
  * returns a set of configurations for tasks based on the current configuration
  *
  * @param maxTasks maximum number of configurations to generate
  * @return configurations for tasks
  */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = List(config.properties.asJava).asJava
}