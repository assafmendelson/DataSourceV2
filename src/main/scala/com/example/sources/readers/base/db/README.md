Reading from database with schema and partitions.
=================================================

The [trivial source example](../../trivial/README.md) shows us how to create the most basic source. Now we will expand on this source by making it query a database (a mock database example, see [the readme](../../../../common/README.md) and [code](../../../../common/MockLegacyDataSourceQuery.scala)). In addition, this example adds support for defining options, supporting multiple partitions and changing the schema.

The description here will repeat some of the logic in [trivial source example](../../trivial/README.md) for clarity.

## DefaultSource

The [full code can be found here](./DefaultSource.scala)

```scala
class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    createReader(StructType(Array(StructField("id", IntegerType))), options)
  }
  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    val numPartitions = options.getInt("num_partitions", 1) // extract the relevant option with a default of 1 partition
    new BaseDbDataSourceReader(numPartitions, schema) // initialize the reader with the relevant extracted options
  }
}
```

DefaultSource is the entry point to the data source. This is the object which is instantiated by Spark when someone attempts to create a dataframe from the data source.

All data sources must extend the DataSourceV2 interface which simply marks them as a data source.

The name of the class must be DefaultSource in order to be able to use the package as the format name.

In this example we implement reader (hence the ```ReadSupport```) trait. 

There are two way to read from a data source:
- By letting the datasource infer the schema itself, e.g.:
```scala
val df = spark.read.format(packageName).load()
```
- By explicitly settings the schema, e.g.:
```scala
val df = spark.read.format(packageName).schema(schema).load()
```

The first option (infer schema) uses the first version of the createReader method (```createReader(options: DataSourceOptions)```) while the second (schema explicitly defined by the caller) uses the second version (```createReader(schema: StructType, options: DataSourceOptions)```).

By default the first version is abstract (i.e. must be implemented) but the second has a default implementation which throws an exception. This means that by default (implementing just the abstract version) the datasource can infer schema and will throw an exception on an explicit one.
In this example, support for both option is given and the schema is actually passed to the DataSourceReader as one of its arguments.

While the trivial example has no options, most (if not all) data sources require some configuration. Many examples can be found in the built-in Spark data sources. For example, the csv data source has options such as "sep" to set the separator and inferSchema to try to figure out the schema as opposed to setting all columns to String.

For the purpose of this example, a single option would be supported: num_partitions. we use the getInt method of the options to extract the value of the option, convert it to integer and set a default value of 1 if the option is not defined.

## DataSourceReader
The [full code can be found here](./BaseDbDataSourceReader.scala)

```scala
class BaseDbDataSourceReader(val numPartitions: Int, val schema: StructType) extends DataSourceReader {
  require(numPartitions > 0, "Number of partitions must be positive")
  require(schema.fields.length == 1, "Only support one field")
  require(schema.fields.head.dataType == IntegerType || schema.fields.head.dataType == FloatType, "Only int and float supported" )

  override def readSchema(): StructType = schema

  private def createSinglePartition(partition: Int): InputPartition[InternalRow] = {
    new BaseDbDataSourcePartition(partition, numPartitions, schema.fields.head.dataType)
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    (0 to numPartitions).map(createSinglePartition).asJava
  }
}
```

A DataSourceReader is responsible for managing the read process. It does so by implementing two methods:
- ```readSchema```: Defines the schema of the dataframe. Since the schema was passed on from the DataSource, we can simply return it.
- ```planInputPartitions```: Used to create a list of partitions. In this case the number of partitions is provided in the constructor and therefore we simply map over it creating a java list of the InputPartition objects. We configure each object with common configuration (numPartitions and data type for the schema) as well as specific configuration (different partition id for each partition)

## InputPartition

```scala
class TrivialDataSourcePartition extends InputPartition[InternalRow]  {
  override def createPartitionReader(): InputPartitionReader[InternalRow]  = new TrivialDataSourcePartitionReader()
}
```
An input partition is responsible for creating the actual data reader (InputPartitionReader) of a single partition of the resulting RDD which is the basis for the dataframe.

While it has generics to define the row, in practice this can only be InternalRow.

The InputPartition is serialized to the worker where it would later create an InputPartitionReader.

The TrivialDataSourcePartition simply creates a TrivialDataSourcePartitionReader

> Note: in Spark 2.3.0 one would extend ```DataReaderFactory[Row]``` instead of ```InputPartition[InternalRow]```. Also, instead of implementing ```override def createPartitionReader(): InputPartitionReader[InternalRow]```, one would implement ```override def createDataReader(): DataReader[Row]```. The logic, however, remained the same.


# InputPartitionReader

```scala
class BaseDbDataSourcePartitionReader(val partition: Int, val numPartitions: Int,
                                      val fieldType: DataType) extends InputPartitionReader[InternalRow] {

  private def filter(rec: Map[String, Int]): Boolean = (rec("id") % numPartitions) == (partition - 1)

  lazy val query: MockLegacyDataSourceQuery = new MockLegacyDataSourceQuery(Seq("id"), filter)

  override def next: Boolean = query.hasNext

  override def get: InternalRow = {
    val v: Int = query.next()("id")
    InternalRow(fieldType match {
      case _: IntegerType => v
      case _: FloatType => v.toFloat
    })
  }

  override def close(): Unit = {
    query.close()
  }
}
}
```

InputPartitionReader is responsible for the actual reading. In the constructor we create a new db connection (which also defines the specific filter for this partition based on the partition id).

next and get are basic iterator behavior with the value from the database being converted to the relevant data type for the schema (see [internal row example](../../internal.row/README.md) for more information on the conversion).

Lastly closing the InputPartitionReader means closing the database connection