package com.agoda.kafka.connector.jdbc.utils

import java.sql.{ResultSet, ResultSetMetaData, Types}

import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.apache.kafka.connect.errors.DataException
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class DataConverterTest extends WordSpec with Matchers with MockitoSugar {

  "Data Converter" should {

    "create schema from result set metadata" in {
      val metaRS = mock[ResultSetMetaData]

      when(metaRS.getColumnCount).thenReturn(3)
      when(metaRS.getColumnName(1)).thenReturn("BOOLEAN")
      when(metaRS.getColumnName(2)).thenReturn("INTEGER")
      when(metaRS.getColumnName(3)).thenReturn("STRING")
      when(metaRS.getColumnLabel(1)).thenReturn("BOOLEAN")
      when(metaRS.getColumnLabel(2)).thenReturn("INTEGER")
      when(metaRS.getColumnLabel(3)).thenReturn("STRING")
      when(metaRS.getColumnType(1)).thenReturn(Types.BOOLEAN)
      when(metaRS.getColumnType(2)).thenReturn(Types.INTEGER)
      when(metaRS.getColumnType(3)).thenReturn(Types.NVARCHAR)
      when(metaRS.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls)
      when(metaRS.isNullable(2)).thenReturn(ResultSetMetaData.columnNullable)
      when(metaRS.isNullable(3)).thenReturn(ResultSetMetaData.columnNullable)

      val s = DataConverter.convertSchema("test-procedure", metaRS)

      s.map(_.name) shouldBe Success("test-procedure")
      s.map(_.field("BOOLEAN").schema()) shouldBe Success(Schema.BOOLEAN_SCHEMA)
      s.map(_.field("INTEGER").schema()) shouldBe Success(Schema.OPTIONAL_INT32_SCHEMA)
      s.map(_.field("STRING").schema()) shouldBe Success(Schema.OPTIONAL_STRING_SCHEMA)
    }

    "skip unsupported SQL types while creating schema" in {
      val metaRS = mock[ResultSetMetaData]

      when(metaRS.getColumnCount).thenReturn(2)
      when(metaRS.getColumnName(1)).thenReturn("KEY")
      when(metaRS.getColumnName(2)).thenReturn("VALUE")
      when(metaRS.getColumnLabel(1)).thenReturn(null)
      when(metaRS.getColumnLabel(2)).thenReturn("")
      when(metaRS.getColumnType(1)).thenReturn(Types.INTEGER)
      when(metaRS.getColumnType(2)).thenReturn(Types.JAVA_OBJECT)
      when(metaRS.isNullable(1)).thenReturn(ResultSetMetaData.columnNoNulls)
      when(metaRS.isNullable(2)).thenReturn(ResultSetMetaData.columnNullableUnknown)

      val s = DataConverter.convertSchema("test-procedure-unsupported", metaRS)

      s.map(_.name) shouldBe Success("test-procedure-unsupported")
      s.map(_.fields.size) shouldBe Success(1)
      s.map(_.field("KEY").schema()) shouldBe Success(Schema.INT32_SCHEMA)
    }

    "convert record from result set" in {
      val rS = mock[ResultSet]
      val metaRS = mock[ResultSetMetaData]

      val builder = SchemaBuilder.struct().name("test")
      builder.field("BOOLEAN", Schema.BOOLEAN_SCHEMA)
      builder.field("INTEGER", Schema.OPTIONAL_INT32_SCHEMA)
      builder.field("STRING", Schema.OPTIONAL_STRING_SCHEMA)
      val schema = builder.build()

      when(rS.getMetaData).thenReturn(metaRS)
      when(metaRS.getColumnCount).thenReturn(3)
      when(metaRS.getColumnName(1)).thenReturn("BOOLEAN")
      when(metaRS.getColumnName(2)).thenReturn("INTEGER")
      when(metaRS.getColumnName(3)).thenReturn("STRING")
      when(metaRS.getColumnLabel(1)).thenReturn("BOOLEAN")
      when(metaRS.getColumnLabel(2)).thenReturn("INTEGER")
      when(metaRS.getColumnLabel(3)).thenReturn("STRING")
      when(metaRS.getColumnType(1)).thenReturn(Types.BOOLEAN)
      when(metaRS.getColumnType(2)).thenReturn(Types.INTEGER)
      when(metaRS.getColumnType(3)).thenReturn(Types.NVARCHAR)
      when(rS.getBoolean(1)).thenReturn(true)
      when(rS.getInt(2)).thenReturn(5)
      when(rS.getNString(3)).thenReturn("Hello World")

      val r = DataConverter.convertRecord(schema, rS)

      r.map(_.schema) shouldBe Success(schema)
      r.map(_.getBoolean("BOOLEAN")) shouldBe Success(true)
      r.map(_.getInt32("INTEGER")) shouldBe Success(5)
      r.map(_.getString("STRING")) shouldBe Success("Hello World")
    }

    "return null for unsupported SQL types while converting record" in {
      val rS = mock[ResultSet]
      val metaRS = mock[ResultSetMetaData]

      val builder = SchemaBuilder.struct().name("test")
      builder.field("KEY", Schema.INT32_SCHEMA)
      builder.field("VALUE", Schema.OPTIONAL_STRING_SCHEMA)
      val schema = builder.build()

      when(rS.getMetaData).thenReturn(metaRS)
      when(metaRS.getColumnCount).thenReturn(2)
      when(metaRS.getColumnName(1)).thenReturn("KEY")
      when(metaRS.getColumnName(2)).thenReturn("VALUE")
      when(metaRS.getColumnLabel(1)).thenReturn("KEY")
      when(metaRS.getColumnLabel(2)).thenReturn("VALUE")
      when(metaRS.getColumnType(1)).thenReturn(Types.INTEGER)
      when(metaRS.getColumnType(2)).thenReturn(Types.NULL)
      when(rS.getInt(1)).thenReturn(5)

      val r = DataConverter.convertRecord(schema, rS)

      r.map(_.schema) shouldBe Success(schema)
      r.map(_.getInt32("KEY")) shouldBe Success(5)
      r.map(_.getString("VALUE")) shouldBe Success(null)
    }

    "fail if schema and result set don't match" in {
      val rS = mock[ResultSet]
      val metaRS = mock[ResultSetMetaData]

      val builder = SchemaBuilder.struct().name("test")
      builder.field("KEY", Schema.INT32_SCHEMA)
      builder.field("VALUE", Schema.BOOLEAN_SCHEMA)
      val schema = builder.build()

      when(rS.getMetaData).thenReturn(metaRS)
      when(metaRS.getColumnCount).thenReturn(2)
      when(metaRS.getColumnName(1)).thenReturn("KEY")
      when(metaRS.getColumnName(2)).thenReturn("VALUE")
      when(metaRS.getColumnLabel(1)).thenReturn("KEY")
      when(metaRS.getColumnLabel(2)).thenReturn("VALUE")
      when(metaRS.getColumnType(1)).thenReturn(Types.INTEGER)
      when(metaRS.getColumnType(2)).thenReturn(Types.DOUBLE)
      when(rS.getInt(1)).thenReturn(5)
      when(rS.getDouble(2)).thenReturn(3.14)

      val r = DataConverter.convertRecord(schema, rS)

      r.isFailure shouldEqual true
      the [DataException] thrownBy r.get
    }
  }
}
