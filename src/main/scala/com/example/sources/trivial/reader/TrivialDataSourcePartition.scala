package com.example.sources.trivial.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
 * Defines a single partition in the dataframe's underlying RDD. This object is generated in the driver and then
 * serialized to the executors where it is responsible for creating the actual the ([[InputPartitionReader]]) which
 * does the actual reading.
 */
class TrivialDataSourcePartition extends InputPartition[InternalRow]  {
  override def createPartitionReader(): InputPartitionReader[InternalRow]  = new TrivialDataSourcePartitionReader()
}