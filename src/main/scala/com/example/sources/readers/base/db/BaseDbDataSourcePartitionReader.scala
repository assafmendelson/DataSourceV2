package com.example.sources.readers.base.db

import com.example.common.DummyLegacySourceQuery

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType

/**
 * This is where we will create the actual db connection as this is instantiated in the executor. In our case we use
 * a static object but in real life we would probably create a new connection.
 *
 * @param partition The id of the specific partition to read
 * @param numPartitions total number of partitions to read
 * @param fieldType: type of the field to use in returned value
 */
class BaseDbDataSourcePartitionReader(val partition: Int, val numPartitions: Int,
                                      val fieldType: DataType) extends InputPartitionReader[InternalRow] {

  // internal method which filters the partition. We include only records where their id modulu number of partitions
  // is the current partition (i.e. round robin between partitions).
  private def filter(rec: Map[String, Int]): Boolean = (rec("id") % numPartitions) == (partition - 1)
  // create the appropriate query. Assume we just take "id"
  lazy val query: DummyLegacySourceQuery = new DummyLegacySourceQuery(Seq("id"), filter)

  override def next: Boolean = query.hasNext

  override def get: InternalRow = {
    // get the next record and from it the "id" column
    val v: Int = query.next()("id")
    // convert the value to the appropriate type based on the schema (since we limit ourselves to integer and float
    // then we can simply use the converted value as input to InternalRow. See com.example.sources.readers.internal.row
    // package for more information on working with internal row.
    InternalRow(fieldType match {
      case _: IntegerType => v
      case _: FloatType => v.toFloat
    })
  }

  override def close(): Unit = {
    query.close()
  }
}