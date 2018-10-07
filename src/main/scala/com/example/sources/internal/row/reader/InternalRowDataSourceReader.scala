package com.example.sources.internal.row.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

/**
 * The InternalRowDataSourceReader uses a static object to define the schema. This is done for simplicity (to avoid the
 * need of moving it inwards to the partitions).
 */
class InternalRowDataSourceReader extends DataSourceReader {
  override def readSchema(): StructType = InternalRowDataSourceReader.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]()
    factoryList.add(new InternalRowDataSourcePartition())
    factoryList
  }
}

object InternalRowDataSourceReader {
  /**
   * A sample schema which has a mix of types. Not all types are supported, this is an example. For more information
   * on the various types see
   * https://github.com/apache/spark/blob/64da2971a1f083926df35fe1366bcba84d97c7b7/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/data/package.scala
   */
  val schema: StructType = {
    StructType(Array(StructField("desc", StringType),
                     StructField("integer", IntegerType),
                     StructField("long", LongType),
                     StructField("timestamp", TimestampType),
                     StructField("float", FloatType),
                     StructField("Array[String]", ArrayType(StringType)),
                     StructField("Array[Long]", ArrayType(StringType)),
                     StructField("boolean", BooleanType)))
  }
}
