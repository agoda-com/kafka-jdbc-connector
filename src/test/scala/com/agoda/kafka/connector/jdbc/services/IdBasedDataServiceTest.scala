package com.agoda.kafka.connector.jdbc.services

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.agoda.kafka.connector.jdbc.JdbcSourceConnectorConstants
import com.agoda.kafka.connector.jdbc.models.DatabaseProduct.{MsSQL, MySQL}
import com.agoda.kafka.connector.jdbc.models.Mode.IncrementingMode
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Success

class IdBasedDataServiceTest extends WordSpec with Matchers with MockitoSugar {

  "ID Based Data Service" should {

    val dataConverter = mock[DataConverter]

    val idBasedDataServiceMssql =
      IdBasedDataService(
        databaseProduct = MsSQL,
        storedProcedureName = "stored-procedure",
        batchSize = 100,
        batchSizeVariableName = "batch-size-variable",
        incrementingVariableName = "incrementing-variable",
        incrementingOffset = 0L,
        incrementingFieldName = "id",
        topic = "id-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter
      )

    val idBasedDataServiceMysql =
      IdBasedDataService(
        databaseProduct = MySQL,
        storedProcedureName = "stored-procedure",
        batchSize = 100,
        batchSizeVariableName = "batch-size-variable",
        incrementingVariableName = "incrementing-variable",
        incrementingOffset = 0L,
        incrementingFieldName = "id",
        topic = "id-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter
      )

    "create correct prepared statement for Mssql" in {
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]

      when(connection.prepareStatement("EXECUTE stored-procedure @incrementing-variable = ?, @batch-size-variable = ?")).thenReturn(statement)
      doNothing().when(statement).setObject(1, 0L)
      doNothing().when(statement).setObject(2, 100)

      idBasedDataServiceMssql.createPreparedStatement(connection)

      verify(connection).prepareStatement("EXECUTE stored-procedure @incrementing-variable = ?, @batch-size-variable = ?")
      verify(statement).setObject(1, 0L)
      verify(statement).setObject(2, 100)
    }

    "create correct prepared statement for Mysql" in {
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]

      when(connection.prepareStatement("CALL stored-procedure (@incrementing-variable := ?, @batch-size-variable := ?)")).thenReturn(statement)
      doNothing().when(statement).setObject(1, 0L)
      doNothing().when(statement).setObject(2, 100)

      idBasedDataServiceMysql.createPreparedStatement(connection)

      verify(connection).prepareStatement("CALL stored-procedure (@incrementing-variable := ?, @batch-size-variable := ?)")
      verify(statement).setObject(1, 0L)
      verify(statement).setObject(2, 100)
    }

    "create correct string representation" in {
      idBasedDataServiceMssql.toString shouldBe
        s"""
           |{
           |   "name" : "IdBasedDataService"
           |   "mode" : "incrementing"
           |   "stored-procedure.name" : "stored-procedure"
           |}
    """.stripMargin
    }

    "extract records" in {
      val resultSet = mock[ResultSet]
      val schema = mock[Schema]
      val field = mock[Field]
      val struct = mock[Struct]

      when(schema.field("id")).thenReturn(field)
      when(field.schema()).thenReturn(Schema.INT32_SCHEMA)
      when(resultSet.next()).thenReturn(true, true, false)
      when(dataConverter.convertRecord(schema, resultSet)).thenReturn(Success(struct))
      when(struct.getInt32("id")).thenReturn(1, 2)


      idBasedDataServiceMssql.extractRecords(resultSet, schema).toString shouldBe
        Success(
          ListBuffer(
            new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> "stored-procedure").asJava,
              Map(IncrementingMode.entryName -> 1).asJava, "id-based-data-topic", schema, struct
            ),
            new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> "stored-procedure").asJava,
              Map(IncrementingMode.entryName -> 2).asJava, "id-based-data-topic", schema, struct
            )
          )
        ).toString
    }
  }
}
