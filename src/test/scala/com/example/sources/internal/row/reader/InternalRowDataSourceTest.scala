package com.example.sources.internal.row.reader

import com.example.common.SharedSparkSession
import org.scalatest.DiagrammedAssertions
import org.scalatest.FunSuite

import org.apache.spark.sql.Row

/**
 * Usage example for using the trivial reader
 */
class InternalRowDataSourceTest extends FunSuite with DiagrammedAssertions with SharedSparkSession {

  test("Some test") {
    // all data sources can be accessed by doing spark.read.format(packageName) where spark is the spark session.
    // Nominally, additional parameters can be added (most commonly options, however in this simple case we simply
    // load in order to get the actual dataframe.
    val df = spark.read.format("com.example.sources.internal.row.reader").load()
    df.printSchema()
    df.show()
    val a: Array[Row] = df.collect()
    val b = a(0).toSeq
    println(b)
/*
    // Simple validation of the schema and values expected. Our simple source has one column: value (which is a string)
    // and includes 5 rows containing the string values 1 to 5
    val expectedSchema = StructType(Array(StructField("value", StringType)))
    assert(df.schema == expectedSchema, "Wrong schema")
    val collected = df.collect().map(row => row.getAs[String]("value")).toSet
    val expected = (1 to 5).map(_.toString).toSet
    assert(collected == expected, "Different results")

    // The trivial source does not support setting the schema ourselves, only the predefined schema
    assertThrows[java.lang.UnsupportedOperationException] {
      spark.read.format("com.example.sources.trivial.reader").schema(expectedSchema).load()
    }
*/
  }

}
