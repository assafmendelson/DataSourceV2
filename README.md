Using Data source V2 in spark 2.4.0.
====================================


## Repository goals

The goal of this repository is to provide an example of how to create a data source for Spark. It uses the new DataSourceV2 and is updated for Spark 2.4.0.

## How is this repository organized
The repository contains different packages under com.example.sources. Each such package represents a different (progressive) example.

The code under the source (src/main) represents the code one would add in order to add the source to their project. Usage can be found under the test (src/test) of the package.

The project is built using sbt and is not aimed to create a real library. Instead it is used just to run the tests (using scalatest) which provide usage example.

In addition to the sources packages, a commons package (com.example.common) package which includes utilities used in multiple examples.

For more information on the commons package see [src/main/scala/com/example/common/README.md](src/main/scala/com/example/common/README.md)

## Example 1: Trivial reader
The purpose of this source is to explain the basics of creating a data source.
It creates a source which always reads the same content which is built-in memory.

For a basic overview of a data source and the simple example see
[src/main/scala/com/example/sources/trivial/reader/README.md](src/main/scala/com/example/trivial/reader/README.md)

## Example 2: Internal Row exploration

The purpose of htis source is to go deeper into the means to create an InternalRow.

See [src/main/scala/com/example/sources/internal/row/reader/README.md](src/main/scala/com/example/internal/row/reader/README.md) for more information.
 

