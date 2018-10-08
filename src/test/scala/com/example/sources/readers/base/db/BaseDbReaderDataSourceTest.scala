package com.example.sources.readers.base.db

import com.example.common.SharedSparkSession
import org.scalatest.DiagrammedAssertions
import org.scalatest.FunSuite

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.spark_partition_id

/**
 * Usage example for using the trivial reader
 */
class BaseDbReaderDataSourceTest extends FunSuite with DiagrammedAssertions with SharedSparkSession {

  test("Some test") {
    val baseReader: DataFrameReader = spark.read.format("com.example.sources.readers.base.db")
    // all data sources can be accessed by doing spark.read.format(packageName) where spark is the spark session.
    // Nominally, additional parameters can be added (most commonly options, however in this simple case we simply
    // load in order to get the actual dataframe.
    val df = baseReader.load()
    df.printSchema()
    df.withColumn("PartitionId", spark_partition_id()).show()

    val dfParallel  = baseReader.option("num_partitions", 3).load()
    dfParallel.withColumn("PartitionId", spark_partition_id()).show()

    val dfFloat = baseReader.schema(StructType(Array(StructField("asFloat", FloatType)))).load()
    dfFloat.printSchema()
    dfFloat.show()
  }

}
