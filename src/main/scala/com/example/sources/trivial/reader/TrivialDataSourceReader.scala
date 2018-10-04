package com.example.sources.trivial.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * A data source reader is responsible to define the schema of the dataframe (readSchema) and to create the underlying
 * RDD partitions (planInputPartitions). The partitions are objects which are serialized to the executors to handle the
 * reading from the worker side.
 */
class TrivialDataSourceReader extends DataSourceReader {
  /**
   * Returns an inferred schema (A constant one in these case)
   */
  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  /**
   * Defines the partitions. This returns a java list (to be compatible with java) of [[InputPartition]] which represent
   * the different partitions of the underlying RDD read.
   */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]()
    factoryList.add(new TrivialDataSourcePartition())
    factoryList
  }
}
