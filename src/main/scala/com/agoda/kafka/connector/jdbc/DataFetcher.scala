package com.agoda.kafka.connector.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait DataFetcher {

  def storedProcedureName: String

  def getRecords(connection: Connection, timeout: Duration): Try[Seq[SourceRecord]] = {
    for {
      preparedStatement <- createPreparedStatement(connection)
      resultSet         <- executeStoredProcedure(preparedStatement, timeout)
      schema            <- DataConverter.convertSchema(storedProcedureName, resultSet.getMetaData)
      records           <- extractRecords(resultSet, schema)
    } yield records
  }

  protected def createPreparedStatement(connection: Connection): Try[PreparedStatement]

  protected def extractRecords(resultSet: ResultSet, schema: Schema): Try[Seq[SourceRecord]]

  private def executeStoredProcedure(preparedStatement: PreparedStatement, timeout: Duration): Try[ResultSet] = Try {
    preparedStatement.setQueryTimeout(timeout.toSeconds.toInt)
    preparedStatement.executeQuery
  }
}
