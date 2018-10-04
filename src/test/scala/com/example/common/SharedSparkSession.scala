package com.example.common

import org.apache.spark.sql.SparkSession

/**
 * Trait to supply a common spark session to all tests
 */
trait SharedSparkSession {
  lazy val spark: SparkSession = SharedSparkSession.spark
}

object SharedSparkSession {
  lazy val spark: SparkSession = {
    val builder = SparkSession.builder()
    // note that we enforce having multiple executors (2) and allow retries.
    builder.master("local[2,2]").appName("Example1").getOrCreate()
  }
}
