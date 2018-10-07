package com.example.sources.trivial.reader

import com.example.common.SharedSparkSession
import org.scalatest.DiagrammedAssertions
import org.scalatest.FunSuite

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Usage example for using the trivial reader
 */
class TrivialDataSourceTest extends FunSuite with DiagrammedAssertions with SharedSparkSession {

  test("Some test") {
    // all data sources can be accessed by doing spark.read.format(packageName) where spark is the spark session.
    // Nominally, additional parameters can be added (most commonly options, however in this simple case we simply
    // load in order to get the actual dataframe.
    val df = spark.read.format("com.example.sources.trivial.reader").load()

    // simple printing of the schema and
    df.printSchema()
    df.show()
  }

}
