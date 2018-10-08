package com.example.sources.readers.internal.row

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.ReadSupport
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

/**
 * The internal row reader aims to expand the concepts in [[com.example.sources.readers.trivial.DefaultSource]] by
 * expanding on the generation of [[org.apache.spark.sql.catalyst.InternalRow]]
 *
 * The overall behavior is almost identical to the trivial data source (com.example.sources.trivial.reader package) with
 * the difference of having a more complex schema with different rows generating it in different ways
 *
 */
class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = new InternalRowDataSourceReader()
}
