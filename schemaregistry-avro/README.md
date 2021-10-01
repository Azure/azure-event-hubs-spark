# azure-schemaregistry-spark (WIP)

## Overview

Schema Registry support in Java is provided by the official Schema Registry SDK in the Azure Java SDK repository.

Schema Registry serializer craft payloads that contain a schema ID and an encoded payload. The ID references a registry-stored schema that can be used to decode the user-specified payload.

However, consuming Schema Registry-backed payloads in Spark is particularly difficult, since - 
- Spark Kafka does not support plug-in with KafkaSerializer and KafkaDeserializer objects, and
- Object management is non-trivial given Spark's driver-executor model.

For these reasons, Spark functions are required to simplify SR UX in Spark.  This repository contains packages that will provide Spark support in Scala for serialization and deserialization of registry-backed payloads.

Currently, only Avro encodings are supported by Azure Schema Registry clients.  `from_avro` and `to_avro` found in the `functions.scala` file will be usable for converting Spark SQL columns from registry-backed payloads to columns of the correct Spark SQL datatype (e.g. `StringType`, `StructType`, etc.).

## Usage

Compile the JAR and build with dependencies using the following Maven command:
```bash
mvn clean compile assembly:assembly
```

The JAR can then be uploaded without additional required dependencies in your environment.  If using `spark-submit`, use the `--jars` option to submit the path of the custom JAR.

## Available API

Both `from_avro` and `to_avro` functions can be used by either providing the schema GUID or the schema itself. Note that if you are providing the schema GUID it should be wrapped in a SchemaGUID object. 
Below you can find more info about available APIs:

```scala
  /**
   * @param data column with SR payloads
   * @param schemaString The avro schema in JSON string format.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @param requireExactSchemaMatch boolean if call should throw if data contents do not exactly match expected schema
   */
  def from_avro(
       data: Column,
       schemaString: String,
       clientOptions: java.util.Map[String, String],
       requireExactSchemaMatch: Boolean = false): Column

  /**
   * @param data column with SR payloads
   * @param schemaId The GUID of the expected schema.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @param requireExactSchemaMatch boolean if call should throw if data contents do not exactly match expected schema
   */
  def from_avro(
       data: Column,
       schemaId: SchemaGUID,
       clientOptions: java.util.Map[String, String],
       requireExactSchemaMatch: Boolean): Column
  
  /**
   * @param data column with SR payloads
   * @param schemaId The GUID of the expected schema.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
  */
  def from_avro(
       data: Column,
       schemaId: SchemaGUID,
       clientOptions: java.util.Map[String, String]): Column

  /**
   * @param data the data column.
   * @param schemaString The avro schema in JSON string format.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   */
  def to_avro(data: Column,
              schemaString: String,
              clientOptions: java.util.Map[java.lang.String, java.lang.String]): Column

  /**
   * @param data the data column.
   * @param schemaId The GUID of the expected schema
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   */
  def to_avro(data: Column,
              schemaId: SchemaGUID,
              clientOptions: java.util.Map[java.lang.String, java.lang.String]): Column

```

You can find examples of how to use the above APIs in 


## Schema Evolution

In the context of stream processing, the primary use case is where the schema GUID references a schema matching in the stream.

However, there are two edge cases that will be common in streaming scenarios in which we are concerned with schema evolution -
- Stream jobs reading old data with new schemas - only backwards compatible data will be readable, meaning that fields may be null.
- Stream jobs reading new data with old schemas - even if the Spark job schema is forwards compatible with the new schema, projecting data written with the new schema to the old one will result in data loss in the case of additional fields being added.

To handle the more dangerous second case, Spark functions will throw if incoming data contains fields that cannot be captured by the existing schema.  This behavior is based on the assumption that perceived data loss is prohibited.

To handle the first case, a parameter will be introduced called `requireExactSchemaMatch`:
- If true, if the schema in the payload is not an exact match to the Spark-specified schema, then the job will throw.  This allows users to specify that their pipeline contain one schema only.
- If false, the job will attempt to read the data incoming in the stream.  In the case of upgraded consumers reading backwards compatible schemas, the job will be able to properly read the schemas (nullable deleted fields, adding new optional fields).

## Failure Modes

Two modes will be supported as dictated by Spark SQL - 
- `FailFastMode` - fail on catching any exception
- `PermissiveMode` - continue processing if parsing exceptions are caught (currently unsupported)

Customers will be able to configure the stream with specific failure models, but the default failure model will be `FailFastMode` to prevent perceived data loss with `PermissiveMode`.

See also:
- aka.ms/schemaregistry
- https://github.com/Azure/azure-schema-registry-for-kafka
