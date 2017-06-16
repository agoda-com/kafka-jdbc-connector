package com.agoda.kafka.connector.jdbc.services

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet}

import com.agoda.kafka.connector.jdbc.JdbcSourceConnectorConstants
import com.agoda.kafka.connector.jdbc.models.Mode.IncrementingMode
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * @constructor
  * @param storedProcedureName name of the stored procedure
  * @param batchSize number of records returned in each batch
  * @param batchSizeVariableName name of the batch size variable in stored procedure
  * @param incrementingVariableName name of the incrementing offset variable in stored procedure
  * @param incrementingOffset value of current incrementing offset
  * @param incrementingFieldName incrementing offset field name in returned records
  * @param topic name of kafka topic where records are stored
  * @param keyFieldOpt optional key field name in returned records
  */
case class IdBasedDataService(storedProcedureName: String,
                              batchSize: Int,
                              batchSizeVariableName: String,
                              incrementingVariableName: String,
                              var incrementingOffset: Long,
                              incrementingFieldName: String,
                              topic: String,
                              keyFieldOpt: Option[String]) extends DataService {

  override protected def createPreparedStatement(connection: Connection): Try[PreparedStatement] = Try {
    val preparedStatement = connection.prepareStatement(
      s"EXECUTE $storedProcedureName @$incrementingVariableName = ?, @$batchSizeVariableName = ?"
    )
    preparedStatement.setObject(1, incrementingOffset)
    preparedStatement.setObject(2, batchSize)
    preparedStatement
  }

  override protected def extractRecords(resultSet: ResultSet, schema: Schema): Try[Seq[SourceRecord]] = Try {
    val sourceRecords = ListBuffer.empty[SourceRecord]
    val idSchemaType = schema.field(incrementingFieldName).schema.`type`()
    var max = incrementingOffset
    while(resultSet.next()) {
      DataConverter.convertRecord(schema, resultSet).map { record =>
        val id = idSchemaType match {
          case Type.INT8  => record.getInt8(incrementingFieldName).toLong
          case Type.INT16 => record.getInt16(incrementingFieldName).toLong
          case Type.INT32 => record.getInt32(incrementingFieldName).toLong
          case Type.INT64 => record.getInt64(incrementingFieldName).toLong
          case _          => throw new IOException("Id field is not of type INT")
        }
        max = if (id > max) id else max

        keyFieldOpt match {
          case Some(keyField) =>
            sourceRecords += new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
              Map(IncrementingMode.entryName -> id).asJava, topic, null, schema, record.get(keyField), schema, record
            )
          case None           =>
            sourceRecords += new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> storedProcedureName).asJava,
              Map(IncrementingMode.entryName -> id).asJava, topic, schema, record
            )
        }
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