Common Utilities Package
========================

This package provides utilities which are used by multiple sources. As such it is a good idea to be familiar with these tools before reading the specific example.

## MockLegacyDataSourceQuery

While some trivial examples can be made using some hard coded data, more complex examples require a data source to connect to.

MockLegacyDataSourceQuery is a dummy data source representing a query. It is possible to create a new instance of it (representing creating a new database connection and doing a query on it), iterating over the results and finally closing it.
For simplicity it has one "table" which contains 20 rows. Each row has 5 columns: id (which is a running number from 1 to 20) and col1 to col4, each containing a number equal to the id + its column number. This provides unique integer values in a table.

When generating the query, a sequence of selected fields is provided (to "select" only some of the fields) and a method from record (a map from field name to field value) to boolean which is used to filter our the results.

The code can be found [here](MockLegacyDataSourceQuery.scala).
## SharedSparkSession

This test trait is used as the base class for all the tests and provide them with a common spark session which is the same for all tests.
The code can be found [here](../../../../../test/scala/com/example/common/SharedSparkSession.scala)