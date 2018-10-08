package com.example.common

/**
 * This class represents a dummy legacy database. Our goal is to create a connector to this database.
 *
 * @param selectedFields A sequence of fieldnames to include in the resulting records. (a "select" clause)
 * @param filter A "where" clause. This is a method between a record and boolean where true means the record is included
 *               in the query result.
 *
 */
class DummyLegacySourceQuery(selectedFields: Seq[String], filter: Map[String, Int] => Boolean)  {
  /**
   * @return true if there are more records in the query results and false otherwise
   */
  def hasNext: Boolean = {
    require(!closed)
    index < records.length
  }

  /** returns the next record as a map from field name to value (for simplicity integer) */
  def next(): Map[String, Int] = {
    require(!closed)
    val rec = records(index)
    index += 1
    rec
  }

  /** closes the connection */
  def close(): Unit = {
    closed = true
  }

  // validates that all selected fields are legal
  require(selectedFields.forall(DummyLegacySourceQuery.fieldNames.contains))

  private val records: Seq[Map[String, Int]] = DummyLegacySourceQuery.allRecords.filter(filter).map(rec => {
    rec.filterKeys(field => selectedFields.contains(field))})

  private var index: Int = 0

  private var closed: Boolean = false
}

object DummyLegacySourceQuery {
  // predefined number of records
  private final val NUM_RECORDS = 20
  // number of extra fields (the table would have an "id" columns and additional NUM_EXTRA_FIELDS columns named col#
  // where # is a number between 1 and NUM_EXTRA_FIELDS
  private final val NUM_EXTRA_FIELDS = 10

  /** A list of all field names in the table */
  lazy val fieldNames: Seq[String] = allRecords.head.keys.toSeq

  /** A sequence of all records in the table. A record is a map between the column name and its (integer) value. */
  private lazy val allRecords: Seq[Map[String, Int]] = (0 until NUM_RECORDS).map(id => {
    // for simplicity a record would be generated so that the value of col# would be id + #.
    Map("id" -> id) ++ (1 until NUM_EXTRA_FIELDS).map(fieldNum => ("col" + fieldNum) -> (id + fieldNum)).toMap
  })
}
