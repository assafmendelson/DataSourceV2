package com.example.sources.readers.trivial

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
    val df = spark.read.format("com.example.sources.readers.trivial").load()

    // simple printing of the schema and
    df.printSchema()
    df.show()

    // The trivial source does not support setting the schema ourselves, only the predefined schema
    val caught = intercept[java.lang.UnsupportedOperationException] {
      val schema = StructType(Array(StructField("value", StringType)))
      spark.read.format("com.example.sources.readers.trivial").schema(schema).load()
    }
    assert(caught.getMessage.endsWith("does not support user specified schema"))
  }

}
