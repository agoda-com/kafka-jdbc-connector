package com.agoda.kafka.connector.jdbc

import com.agoda.kafka.connector.jdbc.Modes.{IncrementingMode, TimestampIncrementingMode, TimestampMode}
import org.scalatest.{Matchers, WordSpec}

class ModesTest extends WordSpec with Matchers {

  "module" should {
    "convert Mode to its string representation" in {
      Modes.getValue(TimestampMode) shouldEqual "timestamp"
      Modes.getValue(IncrementingMode) shouldEqual "incrementing"
      Modes.getValue(TimestampIncrementingMode) shouldEqual "timestamp+incrementing"
    }

    "convert string to corresponding Mode representation" in {
      Modes.getMode("timestamp") shouldBe TimestampMode
      Modes.getMode("incrementing") shouldBe IncrementingMode
      Modes.getMode("timestamp+incrementing") shouldBe TimestampIncrementingMode
    }
  }
}
