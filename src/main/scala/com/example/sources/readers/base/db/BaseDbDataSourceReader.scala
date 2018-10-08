package com.example.sources.readers.base.db

import java.util

import scala.collection.convert.decorateAsJava.seqAsJavaListConverter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType

/**
 * The source reader is responsible for creating the partitions. The readSchema method simply gets the input schema
 * while the planInputPartitions creates the relevant partitions (based on the isParallel option) and passes the
 * relevant schema hint to the actual partition.
 *
 * @param numPartitions The number of partitions to generate
 * @param schema The schema to use
 */
class BaseDbDataSourceReader(val numPartitions: Int, val schema: StructType) extends DataSourceReader {
  require(numPartitions > 0, "Number of partitions must be positive")
  // we only accept schemas which have one field which is either int or float
  require(schema.fields.length == 1, "Only support one field")
  require(schema.fields.head.dataType == IntegerType || schema.fields.head.dataType == FloatType,
          "Only int and float supported" )

  /**
   * Returns the schema of the dataframe. In this case provided as input from the DefaultSource
   */
  override def readSchema(): StructType = schema

  /**
   * Utility method to create the actual partition
   * @param partition The id of the partition to create
   * @return An InputPartition object
   */
  private def createSinglePartition(partition: Int): InputPartition[InternalRow] = {
    new BaseDbDataSourcePartition(partition, numPartitions, schema.fields.head.dataType)
  }

  /**
   * Defines the partitions. This returns a java list (to be compatible with java) of [[InputPartition]] which represent
   * the different partitions of the underlying RDD read.
   */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    (0 to numPartitions).map(createSinglePartition).asJava
  }
}
