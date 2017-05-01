package com.agoda.kafka.connector.jdbc

import java.io.IOException
import java.sql.{ResultSet, ResultSetMetaData, SQLException, Types}
import java.util.{GregorianCalendar, TimeZone}

import org.apache.kafka.connect.data._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object DataConverter {
  private val logger = LoggerFactory.getLogger(classOf[JdbcSourceTask])
  private val UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

  def convertSchema(storedProcedureName: String, metadata: ResultSetMetaData): Schema = {
    val builder = SchemaBuilder.struct.name(storedProcedureName)
    (1 to metadata.getColumnCount).foreach(i => addFieldSchema(metadata, i, builder))
    builder.build
  }

  def convertRecord(schema: Schema, resultSet: ResultSet): Struct = {
    val metadata = resultSet.getMetaData
    val struct = new Struct(schema)
    (1 to metadata.getColumnCount).foreach { i =>
      Try(convertFieldValue(resultSet, i, metadata.getColumnType(i), struct, metadata.getColumnLabel(i))) match {
        case Success(_)               =>
        case Failure(e: IOException)  => logger.warn("Ignoring record because processing failed:", e)
        case Failure(e: SQLException) => logger.warn("Ignoring record due to SQL error:", e)
        case Failure(e)               => logger.warn("Ignoring record due to error", e)
      }
    }
    struct
  }

  private def addFieldSchema(metadata: ResultSetMetaData, col: Int, builder: SchemaBuilder) = {
    val label     = metadata.getColumnLabel(col)
    val name      = metadata.getColumnName(col)
    val fieldName = if (label != null && !label.isEmpty) label else name
    val sqlType   = metadata.getColumnType(col)
    val optional  = metadata.isNullable(col) == ResultSetMetaData.columnNullable ||
                    metadata.isNullable(col) == ResultSetMetaData.columnNullableUnknown

    sqlType match {
      case Types.NULL     =>
        logger.warn("JDBC type {} not currently supported", sqlType)

      case Types.BOOLEAN  =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        else builder.field(fieldName, Schema.BOOLEAN_SCHEMA)

      case Types.BIT | Types.TINYINT  =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_INT8_SCHEMA)
        else builder.field(fieldName, Schema.INT8_SCHEMA)

      case Types.SMALLINT =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_INT16_SCHEMA)
        else builder.field(fieldName, Schema.INT16_SCHEMA)

      case Types.INTEGER  =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA)
        else builder.field(fieldName, Schema.INT32_SCHEMA)

      case Types.BIGINT   =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA)
        else builder.field(fieldName, Schema.INT64_SCHEMA)

      case Types.REAL     =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_FLOAT32_SCHEMA)
        else builder.field(fieldName, Schema.FLOAT32_SCHEMA)

      case Types.FLOAT | Types.DOUBLE   =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA)
        else builder.field(fieldName, Schema.FLOAT64_SCHEMA)

      case Types.NUMERIC | Types.DECIMAL  =>
        val fieldBuilder = Decimal.builder(metadata.getScale(col))
        if (optional) fieldBuilder.optional
        builder.field(fieldName, fieldBuilder.build)

      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR | Types.NVARCHAR |
           Types.LONGNVARCHAR | Types.CLOB | Types.NCLOB | Types.DATALINK | Types.SQLXML =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA)
        else builder.field(fieldName, Schema.STRING_SCHEMA)

      case Types.BINARY | Types.BLOB | Types.VARBINARY | Types.LONGVARBINARY =>
        if (optional) builder.field(fieldName, Schema.OPTIONAL_BYTES_SCHEMA)
        else builder.field(fieldName, Schema.BYTES_SCHEMA)

      case Types.DATE =>
        val dateSchemaBuilder = Date.builder
        if (optional) dateSchemaBuilder.optional
        builder.field(fieldName, dateSchemaBuilder.build)

      case Types.TIME =>
        val timeSchemaBuilder = Time.builder
        if (optional) timeSchemaBuilder.optional
        builder.field(fieldName, timeSchemaBuilder.build)

      case Types.TIMESTAMP =>
        val tsSchemaBuilder = Timestamp.builder
        if (optional) tsSchemaBuilder.optional
        builder.field(fieldName, tsSchemaBuilder.build)

      case Types.ARRAY | Types.OTHER | Types.DISTINCT | Types.STRUCT | Types.REF | Types.ROWID | _ =>
        logger.warn("JDBC type {} not currently supported", sqlType)
    }
  }

  private def convertFieldValue(resultSet: ResultSet, col: Int, colType: Int, struct: Struct, fieldName: String) = {
    val colValue = colType match {
      case Types.NULL => null

      case Types.BOOLEAN => resultSet.getBoolean(col)

      case Types.BIT => resultSet.getByte(col)

      case Types.TINYINT => resultSet.getByte(col)

      case Types.SMALLINT => resultSet.getShort(col)

      case Types.INTEGER => resultSet.getInt(col)

      case Types.BIGINT => resultSet.getLong(col)

      case Types.REAL => resultSet.getFloat(col)

      case Types.FLOAT | Types.DOUBLE => resultSet.getDouble(col)

      case Types.NUMERIC | Types.DECIMAL => resultSet.getBigDecimal(col)

      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR => resultSet.getString(col)

      case Types.NCHAR | Types.NVARCHAR | Types.LONGNVARCHAR => resultSet.getNString(col)

      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => resultSet.getBytes(col)

      case Types.DATE => resultSet.getDate(col, UTC_CALENDAR)

      case Types.TIME => resultSet.getTime(col, UTC_CALENDAR)

      case Types.TIMESTAMP => resultSet.getTimestamp(col, UTC_CALENDAR)

      case Types.DATALINK =>
        val url = resultSet.getURL(col)
        if (url != null) url.toString else null

      case Types.BLOB =>
        val blob = resultSet.getBlob(col)
        val bytes =
          if (blob == null) null
          else if (blob.length > Integer.MAX_VALUE) throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE")
          else blob.getBytes(1, blob.length.toInt)
        blob.free()
        bytes

      case Types.CLOB | Types.NCLOB =>
        val clob = if (colType == Types.CLOB) resultSet.getClob(col) else resultSet.getNClob(col)
        val bytes =
          if(clob == null) null
          else if(clob.length > Integer.MAX_VALUE) throw new IOException("Can't process CLOBs longer than Integer.MAX_VALUE")
          else clob.getSubString(1, clob.length.toInt)
        clob.free()
        bytes

      case Types.SQLXML =>
        val xml = resultSet.getSQLXML(col)
        if (xml != null) xml.getString else null

      case Types.ARRAY | Types.OTHER | Types.DISTINCT | Types.STRUCT | Types.REF | Types.ROWID | _ => null
    }

    struct.put(fieldName, if (resultSet.wasNull) null else colValue)
  }
}