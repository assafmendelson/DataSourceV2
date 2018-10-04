package com.example.sources.trivial.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
 * Created by mendea3 on 10/2/2018.
 */
class TrivialDataSourcePartitionReader extends InputPartitionReader[InternalRow] {
  val values: Array[String] = Array("1", "2", "3", "4", "5")

  var index: Int = 0

  override def next: Boolean = index < values.length

  override def get: InternalRow = {
    val row = InternalRow(org.apache.spark.unsafe.types.UTF8String.fromBytes(values(index).getBytes("UTF-8")))
    index = index + 1
    row
  }

  override def close(): Unit = {}
}