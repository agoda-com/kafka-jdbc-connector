package com.agoda.kafka.connector.jdbc.models

import enumeratum._

import scala.collection.immutable.IndexedSeq

/**
  * Database Product Name
  * ~~~~~~~~~~~~~~~~~~~~~
  *
  * MsSQL :: Microsoft SQL Server.
  *
  * MySQL :: MySQL Server.
  *
  */
sealed abstract class DatabaseProduct(override val entryName: String) extends EnumEntry

object DatabaseProduct extends Enum[DatabaseProduct] {

  val values: IndexedSeq[DatabaseProduct] = findValues

  case object MsSQL extends DatabaseProduct("Microsoft SQL Server")
  case object MySQL extends DatabaseProduct("MySQL")
}