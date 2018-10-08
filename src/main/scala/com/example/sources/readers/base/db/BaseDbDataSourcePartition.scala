package com.example.sources.readers.base.db

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.DataType

/**
 * Since this is the part which is serialized to the workers we want a minimal footprint. For example, a db connection
 * which is needed is not saved as part of the partition but instead as part of the InputPartitionReader.
 *
 * @param partition The id of the specific partition to read
 * @param numPartitions total number of partitions to read
 * @param fieldType: type of the field to use in returned value
 */
class BaseDbDataSourcePartition(val partition: Int, val numPartitions: Int,
                                val fieldType: DataType) extends InputPartition[InternalRow]  {
  /**
   * Returns an input partition reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new BaseDbDataSourcePartitionReader(partition, numPartitions, fieldType)
}