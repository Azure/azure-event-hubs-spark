# azure-schemaregistry-spark

## Overview

Schema Registry support in Java is provided by the official Schema Registry SDK in the Azure Java SDK repository.

Schema Registry serializer craft payloads that contain a schema ID and an encoded payload. The ID references a registry-stored schema that can be used to decode the user-specified payload.

However, consuming Schema Registry-backed payloads in Spark is particularly difficult, since - 
- Spark Kafka does not support plug-in with KafkaSerializer and KafkaDeserializer objects, and
- Object management is non-trivial given Spark's driver-executor model.

For these reasons, Spark functions are required to simplify SR UX in Spark.  This repository contains packages that will provide Spark support in Scala for deserialization of registry-backed payloads.

Currently, only Avro encodings are supported by Azure Schema Registry clients.  `from_avro` found in the `functions.scala` file will be usable for converting Spark SQL columns from registry-backed payloads to columns of the correct Spark SQL datatype (e.g. `StringType`, `StructType`, etc.).

## Usage

Compile the JAR and build with dependencies using the following Maven command:
```bash
mvn clean compile assembly:assembly
```

The JAR can then be uploaded without additional required dependencies in your environment.  If using `spark-submit`, use the `--jars` option to submit the path of the custom JAR.

## Environment Support

| Environment             |Package Version|
|-------------------------|----------------|
| Databricks Runtime 11.X |azure-schemaregistry-spark-avro-1.0.0|
| Databricks Runtime 10.X |azure-schemaregistry-spark-avro-1.0.0|
| Synapse Spark pool 3.2  |azure-schemaregistry-spark-avro-1.0.0|

## Available API

Both `from_avro` functions can be used by either providing the schema GUID or the schema itself. Note that if you are providing the schema GUID it should be wrapped in a SchemaGUID object. 
Below you can find more info about available APIs:

```scala
  /**
   * @param data column with SR payloads
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast) and schema exact match flag
   */
  def from_avro(
       data: Column,
       schemaId: SchemaGUID,
       clientOptions: java.util.Map[String, String]): Column
```

You can find examples of how to use the above APIs in [schema-registry-example](docs/schema-registry-example.md) or [schema-registry-example for pyspark](docs/PySpark/schema-registry-example-pyspark.md) file.

## Failure Modes

Two modes will be supported as dictated by Spark SQL - 
- `FailFastMode` - fail on catching any exception
- `PermissiveMode` - continue processing if parsing exceptions are caught

You can configure the stream with specific failure mode using the `failure.mode` key in the configuration map. The default failure mode is `FailFastMode` to prevent perceived data loss with `PermissiveMode`.

See also:
- aka.ms/schemaregistry
- https://github.com/Azure/azure-schema-registry-for-kafka
