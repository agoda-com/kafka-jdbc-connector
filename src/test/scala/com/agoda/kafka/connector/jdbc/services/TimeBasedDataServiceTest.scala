package com.agoda.kafka.connector.jdbc.services

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.util.{GregorianCalendar, TimeZone}

import com.agoda.kafka.connector.jdbc.models.DatabaseProduct.{MsSQL, MySQL}
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class TimeBasedDataServiceTest extends WordSpec with Matchers with MockitoSugar {

  "Time Based Data Service" should {

    val dataConverter = mock[DataConverter]
    val UTC_Calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

    val timeBasedDataServiceMssql =
      TimeBasedDataService(
        databaseProduct = MsSQL,
        storedProcedureName = "stored-procedure",
        batchSize = 100,
        batchSizeVariableName = "batch-size-variable",
        timestampVariableName = "timestamp-variable",
        timestampOffset = 0L,
        timestampFieldName = "time",
        topic = "timestamp-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter,
        calendar = UTC_Calendar
      )

    val timeBasedDataServiceMysql =
      TimeBasedDataService(
        databaseProduct = MySQL,
        storedProcedureName = "stored-procedure",
        batchSize = 100,
        batchSizeVariableName = "batch-size-variable",
        timestampVariableName = "timestamp-variable",
        timestampOffset = 0L,
        timestampFieldName = "time",
        topic = "time-based-data-topic",
        keyFieldOpt = None,
        dataConverter = dataConverter,
        calendar = UTC_Calendar
      )

    val timestamp = new Timestamp(0L)

    "create correct prepared statement for Mssql" in {
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]

      when(connection.prepareStatement("EXECUTE stored-procedure @timestamp-variable = ?, @batch-size-variable = ?")).thenReturn(statement)
      doNothing().when(statement).setTimestamp(1, timestamp, UTC_Calendar)
      doNothing().when(statement).setObject(2, 100)

      timeBasedDataServiceMssql.createPreparedStatement(connection)

      verify(connection).prepareStatement("EXECUTE stored-procedure @timestamp-variable = ?, @batch-size-variable = ?")
      verify(statement).setTimestamp(1, timestamp, UTC_Calendar)
      verify(statement).setObject(2, 100)
    }

    "create correct prepared statement for Mysql" in {
      val connection = mock[Connection]
      val statement = mock[PreparedStatement]

      when(connection.prepareStatement("CALL stored-procedure (@timestamp-variable := ?, @batch-size-variable := ?)")).thenReturn(statement)
      doNothing().when(statement).setTimestamp(1, timestamp, UTC_Calendar)
      doNothing().when(statement).setObject(2, 100)

      timeBasedDataServiceMysql.createPreparedStatement(connection)

      verify(connection).prepareStatement("CALL stored-procedure (@timestamp-variable := ?, @batch-size-variable := ?)")
      verify(statement).setTimestamp(1, timestamp, UTC_Calendar)
      verify(statement).setObject(2, 100)
    }

    "create correct string representation" in {
      timeBasedDataServiceMssql.toString shouldBe
        s"""
           |{
           |   "name" : "TimeBasedDataService"
           |   "mode" : "timestamp"
           |   "stored-procedure.name" : "stored-procedure"
           |}
    """.stripMargin
    }
  }
}

