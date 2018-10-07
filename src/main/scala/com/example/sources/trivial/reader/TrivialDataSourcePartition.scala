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
  /**
   * Returns an input partition reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createPartitionReader(): InputPartitionReader[InternalRow]  = new TrivialDataSourcePartitionReader()
}