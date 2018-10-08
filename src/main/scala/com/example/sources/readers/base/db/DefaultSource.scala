package com.example.sources.readers.base.db

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.ReadSupport
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * The internal row reader aims to expand the concepts in [[com.example.sources.readers.trivial.DefaultSource]] by
 * simmulating database access and adding some basic functionality.
 *
 * The data source will enable to read from a database (a dummary example database). It will read a single column (id)
 * and will allow setting the number of partitions to read from. This will be configured by adding the "num_partitions"
 * option. If multiple (more than one) partitions are used, the rows will be divided between them equally.
 * In addition, schema so that the values would be a floating point representation instead of an integer representation.
 */
class DefaultSource extends DataSourceV2 with ReadSupport {

  /**
   * Creates a DataSourceReader to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    createReader(StructType(Array(StructField("id", IntegerType))), options)
  }

  /**
   * Creates a DataSourceReader to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * In this example the schema is simply passed to the DataSourceReader and the option should be is_parallel which
   * is defaulted to false.
   *
   * @param schema  the user specified schema.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
   override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    val numPartitions = options.getInt("num_partitions", 1) // extract the relevant option with a default of 1 partition
    new BaseDbDataSourceReader(numPartitions, schema) // initialize the reader with the relevant extracted options
  }
}
