package com.agoda.kafka.connector.jdbc.models

import enumeratum._

import scala.collection.immutable.IndexedSeq

sealed abstract class Mode(override val entryName: String) extends EnumEntry

object Mode extends Enum[Mode] {

  val values: IndexedSeq[Mode] = findValues

  case object TimestampMode extends Mode("timestamp")
  case object IncrementingMode extends Mode("incrementing")
  case object TimestampIncrementingMode extends Mode("timestamp+incrementing")
}
