package com.agoda.kafka.connector.jdbc.services

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.{GregorianCalendar, TimeZone}

import com.agoda.kafka.connector.jdbc.models.DatabaseProduct.{MsSQL, MySQL}
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class TimeIdBasedDataServiceTest extends WordSpec with Matchers with MockitoSugar {

  "Time ID Based Data Service" should {

    val dataConverter = mock[DataConverter]

    val UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

    val idBasedDataServiceMssql =
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
        topic = "id-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter,
        calendar = UTC_CALENDAR
      )

    val idBasedDataServiceMysql =
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
        topic = "id-based-data-topic",
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

      idBasedDataServiceMssql.createPreparedStatement(connection)

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

      idBasedDataServiceMysql.createPreparedStatement(connection)

      verify(connection).prepareStatement("CALL stored-procedure (@timestamp-variable := ?, @incrementing-variable := ?, @batch-size-variable := ?)")
      verify(statement).setTimestamp(1, timestamp, UTC_CALENDAR)
      verify(statement).setObject(2, 0L)
      verify(statement).setObject(3, 100)
    }

    "create correct string representation" in {
      idBasedDataServiceMssql.toString shouldBe
        s"""
           |{
           |   "name" : "TimeIdBasedDataService"
           |   "mode" : "timestamp+incrementing"
           |   "stored-procedure.name" : "stored-procedure"
           |}
    """.stripMargin
    }
  }
}
