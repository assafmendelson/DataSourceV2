package com.example.sources.readers.trivial

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.ReadSupport
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

/**
 * Entry point to the data source. When trying to read from the package, this is the class instantiated.
 * DataSourceV2 trait tells us this is a data source and ReadSupport means it supports reading.
 * Unlike the other classes in the data source, the name of the class MUST be DefaultSource in order to define the
 * data source by package name.
 */
class DefaultSource extends DataSourceV2 with ReadSupport {
  /**
   * The sole method of ReadSupport is the createReader. This creates the appropriate DataSourceReader (in our case
   * ExampleSourceReader)
   * @param options This defines options to the data source such as database connection string. The options is a mapping
   *                from string to string.
   * @return The appropriate data source reader object.
   */
  override def createReader(options: DataSourceOptions): DataSourceReader = new TrivialDataSourceReader()
}
