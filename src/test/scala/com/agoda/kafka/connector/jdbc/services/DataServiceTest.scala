package com.agoda.kafka.connector.jdbc.services

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}

import com.agoda.kafka.connector.jdbc.utils.DataConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.util.Success

class DataServiceTest extends WordSpec with Matchers with MockitoSugar {

  "Data Service" should {

    val spName = "stored-procedure"
    val connection = mock[Connection]
    val converter = mock[DataConverter]
    val sourceRecord1 = mock[SourceRecord]
    val sourceRecord2 = mock[SourceRecord]
    val resultSet = mock[ResultSet]
    val resultSetMetadata = mock[ResultSetMetaData]
    val preparedStatement = mock[PreparedStatement]
    val schema = mock[Schema]

    val dataService = new DataService {

      override def storedProcedureName: String = spName

      override protected def createPreparedStatement(connection: Connection) = Success(preparedStatement)

      override protected def extractRecords(resultSet: ResultSet, schema: Schema) = Success(Seq(sourceRecord1, sourceRecord2))

      override def dataConverter: DataConverter = converter
    }

    "get records" in {
      doNothing().when(preparedStatement).setQueryTimeout(1)
      when(preparedStatement.executeQuery).thenReturn(resultSet)
      when(resultSet.getMetaData).thenReturn(resultSetMetadata)
      when(converter.convertSchema(spName, resultSetMetadata)).thenReturn(Success(schema))

      dataService.getRecords(connection, 1.second) shouldBe Success(Seq(sourceRecord1, sourceRecord2))

      verify(preparedStatement).setQueryTimeout(1)
      verify(preparedStatement).executeQuery
      verify(resultSet).getMetaData
      verify(converter).convertSchema(spName, resultSetMetadata)
    }
  }
}
