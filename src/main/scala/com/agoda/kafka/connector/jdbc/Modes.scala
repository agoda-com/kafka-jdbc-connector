package com.agoda.kafka.connector.jdbc

object Modes {
  sealed trait Mode
  case object TimestampMode extends Mode
  case object IncrementingMode extends Mode
  case object TimestampIncrementingMode extends Mode

  def getValue(mode: Mode): String = mode match {
    case TimestampMode             => "timestamp"
    case IncrementingMode          => "incrementing"
    case TimestampIncrementingMode => "timestamp+incrementing"
  }

  def getMode(mode: String): Mode = mode match {
    case "timestamp"              => TimestampMode
    case "incrementing"           => IncrementingMode
    case "timestamp+incrementing" => TimestampIncrementingMode
  }
}
