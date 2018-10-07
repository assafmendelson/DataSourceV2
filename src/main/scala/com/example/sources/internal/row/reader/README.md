Internal Row data source reading example
========================================

As mentioned in the [trivial example](../../internal/row/reader/README.md) data source, Each row much be converted to InternalRow. This example explores this further.

[This source](../DefaultSource.scala) is similar to the [trivial example](../../internal/row/reader/README.md) with the difference that a complex scehma with the types described below is used. Each row contains a different method of generating the InternalRow to provide a more comprehensive example.

There are two ways to create an InternalRow: By using InternalRows constructors using Spark's internal representation or by using an encoder

## Using Spark's internal representation

The simplest way to create an internal row object is by using the InternalRow constructor (apply or fromSeq):

```scala
InternalRow(val1, val2, val3)
InternalRow.fromSeq(Seq(val1, val2, val3))
```

The values provided must match the schema.

That said, the values must be native to spark's internal data representation.

An overview of the types can be found in: https://github.com/apache/spark/blob/64da2971a1f083926df35fe1366bcba84d97c7b7/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/data/package.scala

### Specific type conversion description
Following are options for specific types:

#### Primitive types
Primitive types are supported natively by the InternalRow construction. This is true both for their primitive (unboxed) version and for their boxed, java object version. Primitive types include int, long, float, double, boolean, short, byte.

A simple example:
```scala
  val schema: StructType = {
    StructType(Array(StructField("integer", IntegerType),
                     StructField("long", LongType),
                     StructField("float", FloatType),
                     StructField("boolean", BooleanType)))
  }

  val row = InternalRow(1, 1L, 1.1f, true)
  val rowFromJava = InternalRow(new Integer(1), new Long(1L), new Float(1.1f), new Boolean(true))
  val rowFromSeq = InternalRow.fromSeq(Seq(1, 1L, 1.1f, true))
```

#### String types

Strings are represented by the ```org.apache.spark.unsafe.types.UTF8String``` class. This represents strings as UTF-8 encoded in the internal Spark formatting.

Regular strings can be converted to UTF8String using the fromString method:

```scala
  val schema: StructType = StructType(Array(StructField("string", StringType)))
  val row = InternalRow(UTF8String.fromString("Some string"))
```

#### Timestamps and dates

Timestamps are passed as long primitive (or java.lang.Long boxed long) representing timestamp in **microseconds** while date is passed as an integer (number of days since 1970)

```scala
  val schema: StructType = StructType(Array(StructField("timestamp", TimestampType), StructField("date", DateType)))
  val row = InternalRow(1516057261000000L, 17547)
```

#### Array types
Array types are a combination of types as the base type of Array and the array type which can be any other type. They are based on ```org.apache.spark.sql.catalyst.util.ArrayData``` class. The array types use the ```ArrayData.toArrayData``` method to generate the array. 

```scala
val schema: StructType = {
    StructType(Array(StructField("Array[String]", ArrayType(StringType)),
                     StructField("Array[Long]", ArrayType(LongType)))
  }

  val row = InternalRow(ArrayData.toArrayData(Array(UTF8String.fromString("1a"), UTF8String.fromString("1b"))),
                        ArrayData.toArrayData(Array[Long](11L, 12L, 13L)))
```

#### Map types
A map is built as two arrays, one for keys and one for values. It is based on the ```org.apache.spark.sql.catalyst.util.ArrayBasedMapData``` (a subclass of ```org.apache.spark.sql.catalyst.util.MapData```).

```scala
  val schema: StructType = StructType(Array(StructField("Map[String, String]", MapType(StringType, StringType)))
  val keyArray = ArrayData.toArrayData(Array(UTF8String.fromString("1a"), UTF8String.fromString("1b")))
  val valArray = ArrayData.toArrayData(Array(UTF8String.fromString("abla"), UTF8String.fromString("bbla")))
  val row = InternalRow(new ArrayBasedMapData(keyArray, valArray))
```

#### Struct types

A struct is implemented by using another InternalRow.

```scala
val schema: StructType = StructType(Array(StructField("Struct of int", StructType(Array(StructField("internalInt", IntegerType)))))
val row = InternalRow(InternalRow(1))
```

### Additional notes

Not all types were covered here, additional types include: DecimalType, BinaryType, NullType and more

In all the above examples, the default nullable option for the schema was used, this means that null is an acceptable value for all values.



## Using an encoder

A second way to create the InternalRow would be to use encoders, i.e.:

```scala
val encoder: ExpressionEncoder[Row] = RowEncoder.apply(InternalRowDataSourceReader.schema).resolveAndBind()

val internalRow = encoder.toRow(Row(val1, val2, val3))
```

This is of course simpler to create and use, however, this my have an adverse effect on performance.

Note that here also, an internal struct is represented by a Row.

