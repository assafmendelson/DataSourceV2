Internal Row data source reading example
========================================

As mentioned in the [trivial example](../../internal/row/reader/README.md) data source, Each row much be converted to InternalRow. This example explores this further.

The InternalRow object has a constructor from both a vararg and a seq, i.e. one can create an internal row inone of the following two ways:
```scala
InternalRow(val1, val2, val3)
InternalRow.fromSeq(Seq(val1, val2, val3))
```

The values provided must match the schema.

That said, the values must be native to spark's internal data representation.

An overview of the types can be found in: https://github.com/apache/spark/blob/64da2971a1f083926df35fe1366bcba84d97c7b7/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/data/package.scala

A second way to create the InternalRow would be to use encoders, i.e.:

```scala
val encoder: ExpressionEncoder[Row] = RowEncoder.apply(InternalRowDataSourceReader.schema).resolveAndBind()

val internalRow = encoder.toRow(Row(val1, val2, val3))
```

This is of course simpler to create and use, however, this my have an adverse effect on performance.