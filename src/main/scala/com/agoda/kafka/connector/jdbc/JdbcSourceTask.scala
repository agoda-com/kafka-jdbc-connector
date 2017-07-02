package com.agoda.kafka.connector.jdbc

import java.sql.{Connection, DriverManager, SQLException}
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.agoda.kafka.connector.jdbc.models.DatabaseProduct
import com.agoda.kafka.connector.jdbc.models.Mode.{IncrementingMode, TimestampIncrementingMode, TimestampMode}
import com.agoda.kafka.connector.jdbc.services.{DataService, IdBasedDataService, TimeBasedDataService, TimeIdBasedDataService}
import com.agoda.kafka.connector.jdbc.utils.Version
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class JdbcSourceTask extends SourceTask {
  private val logger = LoggerFactory.getLogger(classOf[JdbcSourceTask])

  private var config: JdbcSourceTaskConfig  = _
  private var db: Connection                = _
  private var dataService: DataService      = _
  private var running: AtomicBoolean        = _

  override def version(): String = Version.getVersion

/**
  * invoked by kafka-connect runtime to start this task
  *
  * @param props properties required to start this task
  */
  override def start(props: util.Map[String, String]): Unit = {
    Try(new JdbcSourceTaskConfig(props.asScala.toMap)) match {
      case Success(c) => config = c
      case Failure(e) => logger.error("Couldn't start com.agoda.kafka.connector.jdbc.JdbcSourceTask due to configuration error", new ConnectException(e))
    }

    val dbUrl = config.getConnectionUrl
    logger.debug(s"Trying to connect to $dbUrl")
    Try(DriverManager.getConnection(dbUrl)) match {
      case Success(c)               => db = c
      case Failure(e: SQLException) => logger.error(s"Couldn't open connection to $dbUrl : ", e)
                                       throw new ConnectException(e)
      case Failure(e)               => logger.error(s"Couldn't open connection to $dbUrl : ", e)
                                       throw e
    }

    val databaseProduct =  DatabaseProduct.withName(db.getMetaData.getDatabaseProductName)

    val offset = context.offsetStorageReader().offset(
      Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> config.getStoredProcedureName).asJava
    )

    val storedProcedureName         = config.getStoredProcedureName
    val timestampVariableNameOpt    = config.getTimestampVariableName
    val timestampFieldNameOpt       = config.getTimestampFieldName
    val incrementingVariableNameOpt = config.getIncrementingVariableName
    val incrementingFieldNameOpt    = config.getIncrementingFieldName
    val batchSize                   = config.getMaxBatchSize
    val batchSizeVariableName       = config.getMaxBatchSizeVariableName
    val topic                       = config.getTopic
    val keyFieldOpt                 = config.getKeyField

    config.getMode match {
      case TimestampMode =>
        val timestampOffset = Try(offset.get(TimestampMode.entryName)).map(_.toString.toLong).getOrElse(config.getTimestampOffset)
        dataService = TimeBasedDataService(databaseProduct, storedProcedureName, batchSize, batchSizeVariableName,
            timestampVariableNameOpt.get, timestampOffset, timestampFieldNameOpt.get, topic, keyFieldOpt)

      case IncrementingMode =>
        val incrementingOffset = Try(offset.get(IncrementingMode.entryName)).map(_.toString.toLong).getOrElse(config.getIncrementingOffset)
        dataService = IdBasedDataService(databaseProduct, storedProcedureName, batchSize, batchSizeVariableName,
            incrementingVariableNameOpt.get, incrementingOffset, incrementingFieldNameOpt.get, topic, keyFieldOpt)

      case TimestampIncrementingMode =>
        val timestampOffset    = Try(offset.get(TimestampMode.entryName)).map(_.toString.toLong).getOrElse(config.getTimestampOffset)
        val incrementingOffset = Try(offset.get(IncrementingMode.entryName)).map(_.toString.toLong).getOrElse(config.getIncrementingOffset)
        dataService = TimeIdBasedDataService(databaseProduct, storedProcedureName, batchSize, batchSizeVariableName,
            timestampVariableNameOpt.get, timestampOffset, incrementingVariableNameOpt.get, incrementingOffset,
            timestampFieldNameOpt.get, incrementingFieldNameOpt.get, topic, keyFieldOpt)
    }

    running = new AtomicBoolean(true)
  }

/**
  * invoked by kafka-connect runtime to stop this task
  */
  override def stop(): Unit = {
    if (running != null) running.set(false)
    if (db != null) {
      logger.debug("Trying to close database connection")
      Try(db.close()) match {
        case Success(_) =>
        case Failure(e) => logger.error("Failed to close database connection: ", e)
      }
    }
  }

/**
  * invoked by kafka-connect runtime to poll data in [[JdbcSourceConnectorConstants.POLL_INTERVAL_MS_CONFIG]] interval
  */
  override def poll(): util.List[SourceRecord] = this.synchronized { if(running.get) fetchRecords else null }

  private def fetchRecords: util.List[SourceRecord] = {
    logger.debug("Polling new data ...")
    val pollInterval = config.getPollInterval
    val startTime    = System.currentTimeMillis
    val fetchedRecords = dataService.getRecords(db, pollInterval.millis) match {
      case Success(records)                    => if(records.isEmpty) logger.info(s"No updates for $dataService")
                                                  else logger.info(s"Returning ${records.size} records for $dataService")
                                                  records
      case Failure(e: SQLException)            => logger.error(s"Failed to fetch data for $dataService: ", e)
                                                  Seq.empty[SourceRecord]
      case Failure(e: Throwable)               => logger.error(s"Failed to fetch data for $dataService: ", e)
                                                  Seq.empty[SourceRecord]
    }
    val endTime     = System.currentTimeMillis
    val elapsedTime = endTime - startTime

    if(elapsedTime < pollInterval) Thread.sleep(pollInterval - elapsedTime)
    fetchedRecords.asJava
  }
}
