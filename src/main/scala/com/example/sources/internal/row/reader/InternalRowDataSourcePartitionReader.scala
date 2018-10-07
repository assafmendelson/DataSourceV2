package com.example.sources.internal.row.reader

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.util.ArrayData
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
  override def next: Boolean = index < 2

  /**
   * Method to convert a string to the internal representation of UTF8String
   */
  private def fromStr(v: String): UTF8String = UTF8String.fromBytes(v.getBytes("UTF-8"))


  /** Get the next record (row) */
  override def get: InternalRow = {

    // create the internal row in different ways
    val row = index match {
      case 0 => {
        val arrLong: Array[Long] = Array(1L, 2L, 3L)
        InternalRow(fromStr("From native types + conversion"), 1, 2L, 1538676454000000L, 3.1f,
                    ArrayData.toArrayData(Array(fromStr("a"), fromStr("b"))),
                    ArrayData.toArrayData(arrLong),
                                          true)
      }
      case 1 => {
        //encoder.toRow(Row("encoder based", 1, 2L, new Timestamp(1538676454000000L), 3.1f, Array("a", "b"), Array(1, 2), true))
        InternalRow(fromStr("From native types + conversion"), 1, 2L, 1538676454000000L, 3.1f,
                    ArrayData.toArrayData(Array(fromStr("a"), fromStr("b"))), ArrayData.toArrayData(Array(1L, 2L, 3L)),
                    true)
      }
      case _ => throw new Exception("Should never get here")
// todo: Add from java.lang, Add null, use int in long, long in int and the same with java.lang
    }
    index = index + 1
    row
  }

  /** called when done, this is where freeing the database connection can be done */
  override def close(): Unit = {}
}