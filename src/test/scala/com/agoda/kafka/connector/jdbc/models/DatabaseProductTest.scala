package com.agoda.kafka.connector.jdbc.models

import com.agoda.kafka.connector.jdbc.models.DatabaseProduct.{MsSQL, MySQL}
import org.scalatest.{Matchers, WordSpec}

class DatabaseProductTest extends WordSpec with Matchers  {

  "module" should {
    "convert DatabaseProduct to its string representation" in {
      DatabaseProduct.MySQL.entryName shouldEqual "MySQL"
      DatabaseProduct.MsSQL.entryName shouldEqual "Microsoft SQL Server"
    }

    "convert string to corresponding Mode representation" in {
      Mode.withName("MySQL") shouldBe MySQL
      Mode.withName("Microsoft SQL Server") shouldBe MsSQL
    }
  }
}
