package com.agoda.kafka.connector.jdbc

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import java.util.{Date, GregorianCalendar, TimeZone}

import com.agoda.kafka.connector.jdbc.Modes.{IncrementingMode, TimestampMode}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class TimeIdBasedDataFetcher(storedProcedureName: String, batchSize: Int, batchSizeVariableName: String,
                                  timestampVariableName: String, var timestampOffset: Long,
                                  incrementingVariableName: String, var incrementingOffset: Long,
                                  timestampFieldName: String, incrementingFieldName: String, topic: String,
                                  keyFieldOpt: Option[String]
                                 ) extends DataFetcher {
  private val UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

  override def createPreparedStatement(connection: Connection): PreparedStatement = {
    val preparedStatement = connection.prepareStatement(
      s"EXECUTE $storedProcedureName @$timestampVariableName = ?, @$incrementingVariableName = ?, @$batchSizeVariableName = ?"
    )
    preparedStatement.setTimestamp(1, new Timestamp(timestampOffset), UTC_CALENDAR)
    preparedStatement.setObject(2, incrementingOffset)
    preparedStatement.setObject(3, batchSize)
    preparedStatement
  }

  override def extractRecords(resultSet: ResultSet, schema: Schema): Seq[SourceRecord] = {
    val sourceRecords = ListBuffer.empty[SourceRecord]
    var maxTime = timestampOffset
    val idSchemaType = schema.field(incrementingFieldName).schema.`type`()
    var maxId = incrementingOffset
    while (resultSet.next()) {
      val data = DataConverter.convertRecord(schema, resultSet)
      val time = data.get(timestampFieldName).asInstanceOf[Date].getTime
      maxTime = if(time > maxTime) time else maxTime
      val id = idSchemaType match {
        case Type.INT8  => data.getInt8(incrementingFieldName).toLong
        case Type.INT16 => data.getInt16(incrementingFieldName).toLong
        case Type.INT32 => data.getInt32(incrementingFieldName).toLong
        case Type.INT64 => data.getInt64(incrementingFieldName).toLong
        case _          => throw new IOException("Id field is not of type INT")
      }
      maxId = if (id > maxId) id else maxId

      keyFieldOpt match {
        case Some(keyField) =>
          sourceRecords += new SourceRecord(
            Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
            Map(Modes.getValue(TimestampMode) -> time, Modes.getValue(IncrementingMode) -> id).asJava,
            topic, null, schema, data.get(keyField), schema, data
          )
        case None           =>
          sourceRecords += new SourceRecord(
            Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
            Map(Modes.getValue(TimestampMode) -> time, Modes.getValue(IncrementingMode) -> id).asJava,
            topic, schema, data
          )
      }
    }
    timestampOffset = maxTime
    incrementingOffset = maxId
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