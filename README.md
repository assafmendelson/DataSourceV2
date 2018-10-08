Using Data source V2 in spark 2.4.0.
====================================


## Repository goals

The goal of this repository is to provide an example of how to create a data source for Spark. It uses the new DataSourceV2 and is updated for Spark 2.4.0.

## How is this repository organized
The repository contains different packages under com.example.sources. Each such package represents a different (progressive) example.

The code under the source (src/main) represents the code one would add in order to add the source to their project. Usage can be found under the test (src/test) of the package.

The project is built using sbt and is not aimed to create a real library. Instead it is used just to run the tests (using scalatest). These tests are aimed at providing usage example rather than a real test (and as such more often than not print the resulting dataframe instead of doing an actual test)

In addition to the sources packages, a commons package (com.example.common) package which includes utilities used in multiple examples.

For more information on the commons package see [the package description](src/main/scala/com/example/common/README.md)

## Read path

### Example 1: Trivial reader
The purpose of this source is to explain the basics of creating a data source.
It creates a source which always reads the same content which is built-in memory.

For a basic overview of a data source and the simple example see
[the source description](src/main/scala/com/example/sources/readers/trivial/README.md)

### Example 2: Internal Row exploration


The purpose of this source is to go deeper into the means to create an InternalRow.

See [the source description](src/main/scala/com/example/sources/readers/internal/row/README.md) for more information.
 
### Example 3: Reading from database with schema and partitions.

Provides a more realistic example using a mock database and includes configuration through options, supporting multiple partitions and user specified schema.

See [the source description](src/main/scala/com/example/sources/readers/base/db/README.md) for more information.

### Example 4: Predicate pushdown

>=============================TODO=============================

### Example 5: Column pruning

>=============================TODO=============================

### Example 6: Handling retries

>=============================TODO=============================


## Write path

### Trivial example

>=============================TODO=============================

### Base example with database mocking

### Add transactional capabilities to the writing

>=============================TODO=============================

## resources

The following resources can be found to get more information

### Youtube deep dives

- [Spark + AI summit 2018 deep dive](https://www.youtube.com/watch?v=9-eomYXVnvY) (Spark 2.3.0 and plan)
- [meetup video Introduction to Data Source V2 API](https://www.youtube.com/watch?v=Yoc9rLsCV0c) (Spark 2.3.0)
- [meetup video Understanding Transactional Writes in Data Source V2 API](https://www.youtube.com/watch?v=lkYSfgQ_IAY) (Spark 2.3.0)

### online tutorials:
- [Exploring Spark DataSource V2](http://blog.madhukaraphatak.com/categories/datasource-v2-series/) (Spark 2.3.0)

### Relevant Jira:

- [SPARK-15689](https://issues.apache.org/jira/browse/SPARK-15689): Original Data source API v2 (spark 2.3.0)
- Changes for Spark 2.4.0:
  - [SPARK-23323](https://issues.apache.org/jira/browse/SPARK-23323): The output commit coordinator is used by default to ensure only one attempt of each task commits.
  - [SPARK-23325](https://issues.apache.org/jira/browse/SPARK-23325) and [SPARK-24971](https://issues.apache.org/jira/browse/SPARK-24971): Readers should always produce InternalRow instead of Row or UnsafeRow; see SPARK-23325 for detail.
  - [SPARK-24990](https://issues.apache.org/jira/browse/SPARK-24990): ReadSupportWithSchema was removed, the user-supplied schema option was added to ReadSupport.
  - [SPARK-24073](https://issues.apache.org/jira/browse/SPARK-24073): Read splits are now called InputPartition and a few methods were also renamed for clarity.
  - [SPARK-25127](https://issues.apache.org/jira/browse/SPARK-25127): SupportsPushDownCatalystFilters was removed because it leaked Expression in the public API. V2 always uses the Filter API now.
  - [SPARK-24478](https://issues.apache.org/jira/browse/SPARK-24478): Push down is now done when converting the a physical plan.
- Future:
  - [SPARK-25390](https://issues.apache.org/jira/browse/SPARK-25390): Data source V2 refactoring
  - [SPARK-25531](https://issues.apache.org/jira/browse/SPARK-25531): New write APIs for data source v2
  - [SPARK-24252](https://issues.apache.org/jira/browse/SPARK-24252): Add catalog support
  - [SPARK-25186](https://issues.apache.org/jira/browse/SPARK-24252): Stabilize Data Source V2 API

