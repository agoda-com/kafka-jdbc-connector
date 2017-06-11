package com.agoda.kafka.connector.jdbc.models

import com.agoda.kafka.connector.jdbc.models.Mode.{IncrementingMode, TimestampIncrementingMode, TimestampMode}
import org.scalatest.{Matchers, WordSpec}

class ModeTest extends WordSpec with Matchers {

  "module" should {
    "convert Mode to its string representation" in {
      Mode.TimestampMode.entryName shouldEqual "timestamp"
      Mode.IncrementingMode.entryName shouldEqual "incrementing"
      Mode.TimestampIncrementingMode.entryName shouldEqual "timestamp+incrementing"
    }

    "convert string to corresponding Mode representation" in {
      Mode.withName("timestamp") shouldBe TimestampMode
      Mode.withName("incrementing") shouldBe IncrementingMode
      Mode.withName("timestamp+incrementing") shouldBe TimestampIncrementingMode
    }
  }
}
