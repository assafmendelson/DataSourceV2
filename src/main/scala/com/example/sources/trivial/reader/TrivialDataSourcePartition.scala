package com.example.sources.trivial.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

/**
 * Created by mendea3 on 10/2/2018.
 */
class TrivialDataSourcePartition extends InputPartition[InternalRow]  {
  override def createPartitionReader(): InputPartitionReader[InternalRow]  = new TrivialDataSourcePartitionReader()
}