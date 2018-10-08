package com.example.sources.readers.internal.row

import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

/**
 * The iterator which does the actual reading from the data source for the particular partition. This runs on the
 * executor.
 */
class InternalRowDataSourcePartitionReader extends InputPartitionReader[InternalRow] {
  // This encoder is used to convert a row to an internal row. It is easier to use than direct conversion, especially in
  // cases of a complicated schema, however, it is considerably slower and theresfore should not be normally used.
  lazy val encoder: ExpressionEncoder[Row] = RowEncoder.apply(InternalRowDataSourceReader.schema).resolveAndBind()



  var index: Int = 0

  /** Tests if there are any more records in the partition */
  override def next: Boolean = index < 4

  /** Get the next record (row) */
  override def get: InternalRow = {

    // create the internal row in different ways
    val row = index match {
      case 0 => {
        // the basic conversion by using unboxed primitives for primitive values (int, long etc.), microsecond long
        // for timestamp, UTF8String.fromString for all strings and ArrayData.toArrayData with the relevant type for
        // all arrays.
        val keyArray = ArrayData.toArrayData(Array(UTF8String.fromString("1a"), UTF8String.fromString("1b")))
        val valArray = ArrayData.toArrayData(Array(UTF8String.fromString("abla"), UTF8String.fromString("bbla")))
        InternalRow(UTF8String.fromString("Native types + conversion"),11, 12L, 1516057261000000L, 17547, 13.1f,
                    keyArray,
                    ArrayData.toArrayData(Array[Long](11L, 12L, 13L)),
                    new ArrayBasedMapData(keyArray, valArray),
                    InternalRow(1),
                    true)
      }
      case 1 => {
        // it is possible to used boxed versions instead of the unboxed primitives
        InternalRow(UTF8String.fromString("java types + conversion"), new java.lang.Integer(21), new java.lang.Long(22L),
                    new java.lang.Long(1516143661000000L), new java.lang.Integer(17548), new java.lang.Float(23.1f),
                    ArrayData.toArrayData(Array(UTF8String.fromString("2a"), UTF8String.fromString("2b"))),
                    ArrayData.toArrayData(Array[java.lang.Long](new java.lang.Long(21L),
                                                                new java.lang.Long(22L),
                                                                new java.lang.Long(23L))),
                    null,
                    InternalRow(new java.lang.Integer(2)), new java.lang.Boolean(false))
      }
      case 2 => {
        // it is possible to use null values. Note that for empty array an empty array object is passed
        InternalRow(UTF8String.fromString("make everything null except one empty array"), null, null, null, null, null,
                    null, ArrayData.toArrayData(Array.empty[Long]), null, null, null)
      }
      case 3 => {
        // this is by far simplest method assuming the data comes using java objects, especially for complex types.
        // however, this does have a performance penalty.
        encoder.toRow(Row("Encoder based", 31, 32L, new Timestamp(1516230061000L), new Date(1516230061000L), 33.1f,
                          Array("3a", "3b"), Array(31L, 32L, 33L), Map("3a"->"abla", "3b"->"bbla"), Row(3), false))
      }
      case _ => throw new Exception("Should never get here")
    }
    index = index + 1
    row
  }

  /** called when done, this is where freeing the database connection can be done */
  override def close(): Unit = {}
}