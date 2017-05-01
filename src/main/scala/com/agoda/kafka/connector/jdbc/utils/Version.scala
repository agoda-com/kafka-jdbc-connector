package com.agoda.kafka.connector.jdbc.utils

import com.agoda.BuildInfo

object Version {
  lazy val getVersion: String = BuildInfo.version
}
