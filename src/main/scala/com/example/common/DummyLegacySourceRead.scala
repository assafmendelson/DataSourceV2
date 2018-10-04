package com.example.common

import java.sql.Timestamp

/**
 * This object represents a dummy legacy source used for reading.
 *
 * Basic usage would be to call the query method to get an iterator of records. Records are implemented as a map from
 * the field (column) name to value (Any).
 *
 * For simplicity, a predefined sequence of records is created containing unique records with a variety of types. The
 * query would then filter the relevant elements accordingly.
 *
 */
object DummyLegacySourceRead {
  /**
   * A schema of the available fields. The filed name (column name) serves as the key and the value is a string
   * representation of the type. Currently (for the example) the following types are supported: Long, Int, String,
   * Timestamp and Array[String].
   *
   * It can be assumed that the "id" column would be a unique identifier and that the "partition" column has records
   * divided into it by round robin.
   */
  lazy val schema: Map[String, String] = Map("id" -> "Long", "partition" -> "Int", "stringData" -> "String",
                                             "timeData" -> "Timestamp", "arrayData" -> "Array[String]")


  /**
   * Simulates reading a query from the database.
   * @param selectedFields The fields to read
   * @param filter filtering options in the form of a method which receives a record (which is a Map from column name
   *               to value) and returns true if it should be part of the filtering
   * @return An iterator of the records
   */
  def getReadIterator(selectedFields: Seq[String], filter: Map[String, Any] => Boolean): Iterator[Map[String, Any]] = {
    allRecords.filter(filter).map(rec => filterFields(rec, selectedFields)).toIterator
  }

  /**
   *  The number of partitions supported by the database (i.e. assume this is defined as part of the db for simplicity
   */
  val numPartitions: Int = 2

  /** Contain a sequence of all records. This is predefined for simplicity */
  private lazy val allRecords: Seq[Map[String, Any]] = {
    (0L until 1000L).map(id => schema.map { case (k, v) => (k, createValueFromField(k, v, id)) })
  }

  /**
   * Creates a unique field based on the field name, field type, id and number of partitions.
   * @param fieldName The name of the field whose value we calculate
   * @param fieldType The type of the field to create (not all are currently supported)
   * @param idValue The value of the id field
   * @return The value to use
   */
  private def createValueFromField(fieldName: String, fieldType: String, idValue: Long): Any = {
    val baseLong: Long = idValue + fieldName.hashCode // some unique "long" name based on the id and the field
    val baseString: String = fieldName + idValue // some unique "long" name based on the id and the field
    fieldName match {
      case "id" => idValue
      case "partition" => (idValue % numPartitions).toInt // round robin between id and partitions
      case _ => {
        fieldType match {
          case "Long" => baseLong
          case "String" => baseString
          case "Timestamp" => new Timestamp(baseLong)
          case "Array[String]" => Array("A" + baseString, "B" + baseString, "C" + baseString)
          case _ => throw new Exception("Unsupported type. currently supported: Long, String, Timestamp, Array[String]")
        }
      }
    }
  }

  /**
   * Method to filter the fields of a record to include exactly the required ones
   * @param rec The input record to filter
   * @param fields The fields to include
   * @return The modified record
   */
  private def filterFields(rec: Map[String, Any], fields: Seq[String]): Map[String, Any] = {
    // can't have fields which are not part of the record
    require(fields.forall(rec.keySet.contains))
    // remove all fields not part of the requested fields
    rec.filterKeys(fieldName => fields.contains(fieldName))
  }
}
