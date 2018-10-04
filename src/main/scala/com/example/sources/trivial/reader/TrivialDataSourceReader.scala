package com.example.sources.trivial.reader

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * A data source reader is responsible to define the schema of the dataframe (readSchema) and to create the datasource
 * reader factories(createDataReaderFactories). The reader factories are objects which are serialized to the workers. A
 * factory is generated for each partition and is used to create the object that reads the actual partition.
 */
class TrivialDataSourceReader extends DataSourceReader {
  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new TrivialDataSourcePartition())
    factoryList
  }
}
