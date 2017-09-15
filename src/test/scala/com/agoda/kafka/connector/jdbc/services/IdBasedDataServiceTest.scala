package com.agoda.kafka.connector.jdbc.services

import java.sql.{Connection, PreparedStatement}

import com.agoda.kafka.connector.jdbc.models.DatabaseProduct.{MsSQL, MySQL}
import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

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
  }
}
