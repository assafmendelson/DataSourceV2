Trivial data source reading example
===================================

This is the most trivial example for a data source V2.

The data source creates a dataframe with a fixed schema (one column named value which is a string) and contains 5 rows with the values "1", "2", "3", "4" and "5".

Creating a simple data source involves the creation of four classes:

- DefaultSource: The entry point to the data source
- a DataSourceReader: responsible for defining the schema and partitions
- InputPartition: the definition of a single partition (the portion serialized to the workers)
- InputPartitionReader: Does the actual reading of a single partition

The main process is:

**DefaultSource** creates a **DataSourceReader** which creates 1 or more **InputPartitions** which are serialized to the workers where each **InputPartition** creates an **InputPartitionReader** which fills the partition with data from the source


## DefaultSource

```scala
class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = new TrivialDataSourceReader()
}
```

[DefaultSource](./DefaultSource.scala) is the entry point to the data source. This is the object which is instantiated by Spark when someone attempts to create a dataframe from the data source.

All data sources must extend the DataSourceV2 interface which simply marks them as a data source.

The name of the class must be DefaultSource in order to be able to use the package as the format name.

In this simple example the data source should support simple reading (i.e. ```spark.read.format(packagename)```) and therefore implements the ReadSupport trait. A more generic data source may implement additional traits (e.g. for writing)

The ReadSupport trait requires a createReader method which receives options and returns a DataSourceReader. This enables one to do something like ```spark.read.format(packagename).option("someKey", "someValue")```. In this example, for simplicity we ignore the options.


## DataSourceReader

```scala
class TrivialDataSourceReader extends DataSourceReader {
  override def readSchema(): StructType = StructType(Array(StructField("value", StringType)))

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new TrivialDataSourcePartition())
    factoryList
  }
}
```

A DataSourceReader is responsible for managing the read process. It does so by implementing two methods:
- readSchema: Used to infer the schema of the dataframe (StructType)
- planInputPartitions: Used to create a list of partitions

The [TrivialDataSourceReader](./TrivialDataSourceReader.scala) implementation extends DataSourceReader and implements these two methods.
- It's schema is a simple constant schema.

- planInputPartitions generates a list of partitions.
  - The list is java.util.List for easier java integration.
  - The different partitions object will later be serialized to the workers. Here we are generating only a single partition of type TrivialDataSourcePartition which represent a single partition in our dummy source

> Note: The signature of ```DataSourceReader.planInputPartitions``` changed in Spark 2.4.0. In Spark 2.3.0 it was called ```createDataReaderFactories``` and returned a list of ```DataReaderFactory[Row]``` instead of ```InputPartition[InternalRow]```. The logic, however, remained the same.

## InputPartition

```scala
class TrivialDataSourcePartition extends InputPartition[InternalRow]  {
  override def createPartitionReader(): InputPartitionReader[InternalRow]  = new TrivialDataSourcePartitionReader()
}
```
An input partition is responsible for creating the actual data reader (InputPartitionReader) of a single partition of the resulting RDD which is the basis for the dataframe.

While it has generics to define the row, in practice this can only be InternalRow.

The InputPartition is serialized to the worker where it would later create an InputPartitionReader.

The [TrivialDataSourcePartition](./TrivialDataSourcePartition.scala) implementation simply creates a TrivialDataSourcePartitionReader

> Note: in Spark 2.3.0 one would extend ```DataReaderFactory[Row]``` instead of ```InputPartition[InternalRow]```. Also, instead of implementing ```override def createPartitionReader(): InputPartitionReader[InternalRow]```, one would implement ```override def createDataReader(): DataReader[Row]```. The logic, however, remained the same.


# InputPartitionReader

```scala
class TrivialDataSourcePartitionReader extends InputPartitionReader[InternalRow] {
  val values: Array[String] = Array("1", "2", "3", "4", "5")

  var index: Int = 0

  override def next: Boolean = index < values.length

  override def get: InternalRow = {
    val row = InternalRow(org.apache.spark.unsafe.types.UTF8String.fromBytes(values(index).getBytes("UTF-8")))
    index = index + 1
    row
  }

  override def close(): Unit = {}
}
```

[TrivialDataSourcePartitionReader](./TrivialDataSourcePartitionReader.scala) is an iterator of internal row, i.e. the following methods should be implemented:
- next: A Boolean whether or not there are more rows
- get: Returns the next row
- close: used on the end so we can close any connections.

Beyond a basic iterator implementation, the important issue to note here is the fact that it iterates on **InternalRow**. THe constructor of InternalRow assumes relevant **Internal** Spark types. In this example, it needs to receive ```org.apache.spark.unsafe.types.UTF8String``` instead of a regular string.

> Note: This is perhaps the biggest change from Spark 2.3.0 where an iterator of Row was used which enabled the use of regular java objects (i.e. String was automatically converted behind the scenes). In addition, the name of the base class was changed from ```DataReader[Row]``` in Spark 2.3.0 to ```InputPartitionReader[InternalRow]``` in Spark 2.4.0