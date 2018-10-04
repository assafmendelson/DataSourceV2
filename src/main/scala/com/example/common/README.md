Common Utilities Package
========================

This package provides utilities which are used by multiple sources. As such it is a good idea to be familiar with these tools before reading the specific example.

## DummyLegacySourceRead

While some trivial examples can be made using some hard coded data, more complex examples require a data source to connect to.

DummyLegacySourceRead is a dummy data source for reading. It has hard coded data (1000 records with various fields of different types) which can be queries by setting the selected fields and supplying a method from record to boolean to filter them.

## SharedSparkSession

This test trait is used as the base class for all the tests and provide them with a common spark session which is the same for all tests.