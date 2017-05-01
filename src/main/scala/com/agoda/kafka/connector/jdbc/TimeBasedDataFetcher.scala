package com.agoda.kafka.connector.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import java.util.{Date, GregorianCalendar, TimeZone}

import com.agoda.kafka.connector.jdbc.Modes.TimestampMode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class TimeBasedDataFetcher(storedProcedureName: String, batchSize: Int, batchSizeVariableName: String,
                                timestampVariableName: String, var timestampOffset: Long, timestampFieldName: String,
                                topic: String, keyFieldOpt: Option[String]) extends DataFetcher {
  private val UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

  override protected def createPreparedStatement(connection: Connection): PreparedStatement = {
    val preparedStatement = connection.prepareStatement(
      s"EXECUTE $storedProcedureName @$timestampVariableName = ?, @$batchSizeVariableName = ?"
    )
    preparedStatement.setTimestamp(1, new Timestamp(timestampOffset), UTC_CALENDAR)
    preparedStatement.setObject(2, batchSize)
    preparedStatement
  }

  override protected def extractRecords(resultSet: ResultSet, schema: Schema): Seq[SourceRecord] = {
    val sourceRecords = ListBuffer.empty[SourceRecord]
    var max = timestampOffset
    while (resultSet.next()) {
      val data = DataConverter.convertRecord(schema, resultSet)
      val time = data.get(timestampFieldName).asInstanceOf[Date].getTime
      max = if(time > max) time else max
      keyFieldOpt match {
        case Some(keyField) =>
          sourceRecords += new SourceRecord(
            Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
            Map(Modes.getValue(TimestampMode) -> time).asJava, topic, null, schema, data.get(keyField), schema, data
          )
        case None           =>
          sourceRecords += new SourceRecord(
            Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
            Map(Modes.getValue(TimestampMode) -> time).asJava, topic, schema, data
          )
      }
    }
    timestampOffset = max
    sourceRecords
  }

  override def toString: String = {
    s"""
       |{
       |   "name" : ${this.getClass.getSimpleName}
       |   "stored-procedure.name" : $storedProcedureName
       |}
    """.stripMargin
  }
}