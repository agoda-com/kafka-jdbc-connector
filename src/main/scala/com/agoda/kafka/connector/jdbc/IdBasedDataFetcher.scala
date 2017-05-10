package com.agoda.kafka.connector.jdbc

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet}

import com.agoda.kafka.connector.jdbc.models.Mode.IncrementingMode
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class IdBasedDataFetcher(storedProcedureName: String, batchSize: Int, batchSizeVariableName: String,
                              incrementingVariableName: String, var incrementingOffset: Long, incrementingFieldName: String,
                              topic: String, keyFieldOpt: Option[String]) extends DataFetcher {

  override protected def createPreparedStatement(connection: Connection): PreparedStatement = {
    val preparedStatement = connection.prepareStatement(
      s"EXECUTE $storedProcedureName @$incrementingVariableName = ?, @$batchSizeVariableName = ?"
    )
    preparedStatement.setObject(1, incrementingOffset)
    preparedStatement.setObject(2, batchSize)
    preparedStatement
  }

  override protected def extractRecords(resultSet: ResultSet, schema: Schema): ListBuffer[SourceRecord] = {
    val sourceRecords = ListBuffer.empty[SourceRecord]
    val idSchemaType = schema.field(incrementingFieldName).schema.`type`()
    var max = incrementingOffset
    while(resultSet.next()) {
      val data = DataConverter.convertRecord(schema, resultSet)
      val id = idSchemaType match {
        case Type.INT8  => data.getInt8(incrementingFieldName).toLong
        case Type.INT16 => data.getInt16(incrementingFieldName).toLong
        case Type.INT32 => data.getInt32(incrementingFieldName).toLong
        case Type.INT64 => data.getInt64(incrementingFieldName).toLong
        case _          => throw new IOException("Id field is not of type INT")
      }
      max = if (id > max) id else max

      keyFieldOpt match {
        case Some(keyField) =>
          sourceRecords += new SourceRecord(
            Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
            Map(IncrementingMode.entryName -> id).asJava, topic, null, schema, data.get(keyField), schema, data
          )
        case None           =>
          sourceRecords += new SourceRecord(
            Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
            Map(IncrementingMode.entryName -> id).asJava, topic, schema, data
          )
      }
    }
    incrementingOffset = max
    sourceRecords
  }

  override def toString: String = {
    s"""
       |{
       |   "name" : ${this.getClass.getSimpleName}
       |   "mode" : ${IncrementingMode.entryName}
       |   "stored-procedure.name" : $storedProcedureName
       |}
    """.stripMargin
  }
}