package com.agoda.kafka.connector.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait DataFetcher {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def storedProcedureName: String

  def getRecords(connection: Connection, timeout: Duration): Seq[SourceRecord] = {
    val preparedStatement = createPreparedStatement(connection)

    val resultSet = Try(executeStoredProcedure(preparedStatement, timeout)) match {
      case Success(r) => r
      case Failure(e) => logger.error(e.getMessage, e); throw e
    }

    val schema = Try(DataConverter.convertSchema(storedProcedureName, resultSet.getMetaData)) match {
      case Success(s) => s
      case Failure(e) => logger.error(e.getMessage, e); throw e
    }

    Try(extractRecords(resultSet, schema)) match {
      case Success(r) => r
      case Failure(e) => logger.error(e.getMessage, e); throw e
    }
  }

  protected def createPreparedStatement(connection: Connection): PreparedStatement

  protected def extractRecords(resultSet: ResultSet, schema: Schema): Seq[SourceRecord]

  private def executeStoredProcedure(preparedStatement: PreparedStatement, timeout: Duration): ResultSet = {
    preparedStatement.setQueryTimeout(timeout.toSeconds.toInt)
    preparedStatement.executeQuery
  }
}
