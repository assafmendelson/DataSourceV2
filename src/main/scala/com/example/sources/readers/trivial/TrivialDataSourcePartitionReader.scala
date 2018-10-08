package com.example.sources.readers.trivial

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
 * The iterator which does the actual reading from the data source for the particular partition. This runs on the
 * executor.
 */
class TrivialDataSourcePartitionReader extends InputPartitionReader[InternalRow] {
  // some instantiation. In this case we create a constant array to run through but in a real data source this would
  // be where any initialization would occur such as creating a database connection and running the basic query
  val values: Array[String] = Array("1", "2", "3", "4", "5")

  var index: Int = 0

  /** Tests if there are any more records in the partition */
  override def next: Boolean = index < values.length

  /** Get the next record (row) */
  override def get: InternalRow = {
    val row = InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString(values(index)))
    index = index + 1
    row
  }

  /** called when done, this is where freeing the database connection can be done */
  override def close(): Unit = {}
}