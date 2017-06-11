package com.agoda.kafka.connector.jdbc.services

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import java.util.{Date, GregorianCalendar, TimeZone}

import com.agoda.kafka.connector.jdbc.JdbcSourceConnectorConstants
import com.agoda.kafka.connector.jdbc.models.Mode.{IncrementingMode, TimestampMode}
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Try

case class TimeIdBasedDataService(storedProcedureName: String, batchSize: Int, batchSizeVariableName: String,
                                  timestampVariableName: String, var timestampOffset: Long,
                                  incrementingVariableName: String, var incrementingOffset: Long,
                                  timestampFieldName: String, incrementingFieldName: String, topic: String,
                                  keyFieldOpt: Option[String]) extends DataService {

  private val UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

  override def createPreparedStatement(connection: Connection): Try[PreparedStatement] = Try {
    val preparedStatement = connection.prepareStatement(
      s"EXECUTE $storedProcedureName @$timestampVariableName = ?, @$incrementingVariableName = ?, @$batchSizeVariableName = ?"
    )
    preparedStatement.setTimestamp(1, new Timestamp(timestampOffset), UTC_CALENDAR)
    preparedStatement.setObject(2, incrementingOffset)
    preparedStatement.setObject(3, batchSize)
    preparedStatement
  }

  override def extractRecords(resultSet: ResultSet, schema: Schema): Try[Seq[SourceRecord]] = Try {
    val sourceRecords = ListBuffer.empty[SourceRecord]
    var maxTime = timestampOffset
    var maxId = incrementingOffset
    val idSchemaType = schema.field(incrementingFieldName).schema.`type`()
    while (resultSet.next()) {
      DataConverter.convertRecord(schema, resultSet) map { record =>
        val time = record.get(timestampFieldName).asInstanceOf[Date].getTime
        maxTime = if(time > maxTime) time else maxTime
        val id = idSchemaType match {
          case Type.INT8  => record.getInt8(incrementingFieldName).toLong
          case Type.INT16 => record.getInt16(incrementingFieldName).toLong
          case Type.INT32 => record.getInt32(incrementingFieldName).toLong
          case Type.INT64 => record.getInt64(incrementingFieldName).toLong
          case _          => throw new IOException("Id field is not of type INT")
        }
        maxId = if (id > maxId) id else maxId

        keyFieldOpt match {
          case Some(keyField) =>
            sourceRecords += new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
              Map(TimestampMode.entryName -> time, IncrementingMode.entryName -> id).asJava,
              topic, null, schema, record.get(keyField), schema, record
            )
          case None           =>
            sourceRecords += new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
              Map(TimestampMode.entryName -> time, IncrementingMode.entryName -> id).asJava,
              topic, schema, record
            )
        }
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
       |   "mode" : ${TimestampMode.entryName}+${IncrementingMode.entryName}
       |   "stored-procedure.name" : $storedProcedureName
       |}
    """.stripMargin
  }
}