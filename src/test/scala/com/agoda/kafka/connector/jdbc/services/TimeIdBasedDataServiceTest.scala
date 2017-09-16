package com.agoda.kafka.connector.jdbc.services

import java.sql._
import java.util.{GregorianCalendar, TimeZone}

import com.agoda.kafka.connector.jdbc.JdbcSourceConnectorConstants
import com.agoda.kafka.connector.jdbc.models.DatabaseProduct.{MsSQL, MySQL}
import com.agoda.kafka.connector.jdbc.models.Mode.{IncrementingMode, TimestampMode}
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Success

class TimeIdBasedDataServiceTest extends WordSpec with Matchers with MockitoSugar {

  "Time ID Based Data Service" should {

    val dataConverter = mock[DataConverter]

    val UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

    val timeIdBasedDataServiceMssql =
      TimeIdBasedDataService(
        databaseProduct = MsSQL,
        storedProcedureName = "stored-procedure",
        batchSize = 100,
        batchSizeVariableName = "batch-size-variable",
        timestampVariableName = "timestamp-variable",
        timestampOffset = 0L,
        timestampFieldName = "time",
        incrementingVariableName = "incrementing-variable",
        incrementingOffset = 0L,
        incrementingFieldName = "id",
        topic = "time-id-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter,
        calendar = UTC_CALENDAR
      )

    val timeIdBasedDataServiceMysql =
      TimeIdBasedDataService(
        databaseProduct = MySQL,
        storedProcedureName = "stored-procedure",
        batchSize = 100,
        batchSizeVariableName = "batch-size-variable",
        timestampVariableName = "timestamp-variable",
        timestampOffset = 0L,
        timestampFieldName = "time",
        incrementingVariableName = "incrementing-variable",
        incrementingOffset = 0L,
        incrementingFieldName = "id",
        topic = "time-id-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter,
        calendar = UTC_CALENDAR
      )

    val timestamp = new Timestamp(0L)

    "create correct prepared statement for Mssql" in {
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]

      when(connection.prepareStatement("EXECUTE stored-procedure @timestamp-variable = ?, @incrementing-variable = ?, @batch-size-variable = ?")).thenReturn(statement)
      doNothing().when(statement).setTimestamp(1, timestamp, UTC_CALENDAR)
      doNothing().when(statement).setObject(2, 0L)
      doNothing().when(statement).setObject(3, 100)

      timeIdBasedDataServiceMssql.createPreparedStatement(connection)

      verify(connection).prepareStatement("EXECUTE stored-procedure @timestamp-variable = ?, @incrementing-variable = ?, @batch-size-variable = ?")
      verify(statement).setTimestamp(1, timestamp, UTC_CALENDAR)
      verify(statement).setObject(2, 0L)
      verify(statement).setObject(3, 100)
    }

    "create correct prepared statement for Mysql" in {
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]

      when(connection.prepareStatement("CALL stored-procedure (@timestamp-variable := ?, @incrementing-variable := ?, @batch-size-variable := ?)")).thenReturn(statement)
      doNothing().when(statement).setTimestamp(1, timestamp, UTC_CALENDAR)
      doNothing().when(statement).setObject(2, 0L)
      doNothing().when(statement).setObject(3, 100)

      timeIdBasedDataServiceMysql.createPreparedStatement(connection)

      verify(connection).prepareStatement("CALL stored-procedure (@timestamp-variable := ?, @incrementing-variable := ?, @batch-size-variable := ?)")
      verify(statement).setTimestamp(1, timestamp, UTC_CALENDAR)
      verify(statement).setObject(2, 0L)
      verify(statement).setObject(3, 100)
    }

    "create correct string representation" in {
      timeIdBasedDataServiceMssql.toString shouldBe
        s"""
           |{
           |   "name" : "TimeIdBasedDataService"
           |   "mode" : "timestamp+incrementing"
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
      when(struct.get("time")).thenReturn(new Date(1L), new Date(2L))

      timeIdBasedDataServiceMssql.extractRecords(resultSet, schema).toString shouldBe
        Success(
          ListBuffer(
            new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> "stored-procedure").asJava,
              Map(TimestampMode.entryName -> 1L, IncrementingMode.entryName -> 1).asJava,
              "time-id-based-data-topic", schema, struct
            ),
            new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> "stored-procedure").asJava,
              Map(TimestampMode.entryName -> 2L, IncrementingMode.entryName -> 2).asJava,
              "time-id-based-data-topic", schema, struct
            )
          )
        ).toString
      timeIdBasedDataServiceMssql.timestampOffset shouldBe 2L
      timeIdBasedDataServiceMssql.incrementingOffset shouldBe 2
    }

    "don't change offset when the record ids and timestamps are smaller than offset id" in {
      val timeIdBasedDataServiceWithLargerOffsetMssql = timeIdBasedDataServiceMssql.copy(incrementingOffset = 3, timestampOffset = 3L)
      val resultSet = mock[ResultSet]
      val schema = mock[Schema]
      val field = mock[Field]
      val struct = mock[Struct]

      when(schema.field("id")).thenReturn(field)
      when(field.schema()).thenReturn(Schema.INT32_SCHEMA)
      when(resultSet.next()).thenReturn(true, true, false)
      when(dataConverter.convertRecord(schema, resultSet)).thenReturn(Success(struct))
      when(struct.getInt32("id")).thenReturn(1, 2)
      when(struct.get("time")).thenReturn(new Date(1L), new Date(2L))

      timeIdBasedDataServiceWithLargerOffsetMssql.extractRecords(resultSet, schema)

      timeIdBasedDataServiceWithLargerOffsetMssql.timestampOffset shouldBe 3L
      timeIdBasedDataServiceWithLargerOffsetMssql.incrementingOffset shouldBe 3
    }

    "extract records with key" in {
      val timeIdBasedDataServiceWithKeyMysql = timeIdBasedDataServiceMysql.copy(keyFieldOpt = Some("key"))
      val resultSet = mock[ResultSet]
      val schema = mock[Schema]
      val field = mock[Field]
      val struct = mock[Struct]

      when(schema.field("id")).thenReturn(field)
      when(field.schema()).thenReturn(Schema.INT32_SCHEMA)
      when(resultSet.next()).thenReturn(true, true, false)
      when(dataConverter.convertRecord(schema, resultSet)).thenReturn(Success(struct))
      when(struct.getInt32("id")).thenReturn(1, 2)
      when(struct.get("time")).thenReturn(new Date(1L), new Date(2L))
      when(struct.get("key")).thenReturn("key-1", "key-2")

      timeIdBasedDataServiceWithKeyMysql.extractRecords(resultSet, schema).toString shouldBe
        Success(
          ListBuffer(
            new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> "stored-procedure").asJava,
              Map(TimestampMode.entryName -> 1L, IncrementingMode.entryName -> 1).asJava,
              "time-id-based-data-topic", null, schema, "key-1", schema, struct
            ),
            new SourceRecord(
              Map(JdbcSourceConnectorConstants.STORED_PROCEDURE_NAME_KEY -> "stored-procedure").asJava,
              Map(TimestampMode.entryName -> 2L, IncrementingMode.entryName -> 2).asJava,
              "time-id-based-data-topic", null, schema, "key-2", schema, struct
            )
          )
        ).toString
    }

    "returns no record when id field is not of type integer" in {
      val resultSet = mock[ResultSet]
      val schema = mock[Schema]
      val field = mock[Field]
      val struct = mock[Struct]

      when(schema.field("id")).thenReturn(field)
      when(field.schema()).thenReturn(Schema.STRING_SCHEMA)
      when(resultSet.next()).thenReturn(true, false)
      when(dataConverter.convertRecord(schema, resultSet)).thenReturn(Success(struct))
      when(struct.get("time")).thenReturn(new Date(1L), Nil)

      timeIdBasedDataServiceMysql.extractRecords(resultSet, schema) shouldBe Success(ListBuffer())
    }
  }
}
