/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.schemaregistry.spark.avro

import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializer}
import scala.collection.JavaConverters._
import org.apache.spark.sql.Column

/***
 * Scala object containing utility methods for serialization/deserialization with Azure Schema Registry and Spark SQL
 * columns.
 *
 * Functions are agnostic to data source or sink and can be used with any Schema Registry payloads, including:
 * - Kafka Spark connector ($value)
 * - Event Hubs Spark connector ($Body)
 * - Event Hubs Avro Capture blobs ($Body)
 */
object functions {

  val SCHEMA_REGISTRY_TENANT_ID_KEY: String = "schema.registry.tenant.id"
  val SCHEMA_REGISTRY_CLIENT_ID_KEY: String = "schema.registry.client.id"
  val SCHEMA_REGISTRY_CLIENT_SECRET_KEY: String = "schema.registry.client.secret"
  val SCHEMA_REGISTRY_URL: String = "schema.registry.url"
  val SCHEMA_GROUP_KEY: String = "schema.group"
  val SCHEMA_NAME_KEY: String = "schema.name"
  val SCHEMA_AUTO_REGISTER_FLAG_KEY: String = "schema.auto.register.flag"
  val SCHEMA_PARSE_MODE: String = "mode"

  /***
   * Converts Spark SQL Column containing SR payloads into a into its corresponding catalyst value.
   * This methods either uses schema GUID or the schema in JSON string format.
   * If schemaId is provided, it must be the Schema GUID of the actual schema used to serialize the data.
   * If jsonFormatSchema is provided, it must match the actual schema used to serialize the data.
   *
   * @param data column with SR payloads
   * @param schemaString The avro schema in JSON string format.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @param requireExactSchemaMatch boolean if call should throw if data contents do not exactly match expected schema
   */
  def from_avro(
       data: Column,
       schemaString: String,
       clientOptions: java.util.Map[String, String],
       requireExactSchemaMatch: Boolean = false): Column = {
    new Column(AvroDataToCatalyst(data.expr, SchemaReader.VALUE_NOT_PROVIDED, schemaString, clientOptions.asScala.toMap, requireExactSchemaMatch))
  }

  /***
   * Converts Spark SQL Column containing SR payloads into a into its corresponding catalyst value.
   * This methods either uses schema GUID or the schema in JSON string format.
   * If schemaId is provided, it must be the Schema GUID of the actual schema used to serialize the data.
   * If jsonFormatSchema is provided, it must match the actual schema used to serialize the data.
   *
   * @param data column with SR payloads
   * @param schemaId The GUID of the expected schema.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @param requireExactSchemaMatch boolean if call should throw if data contents do not exactly match expected schema
   */
  def from_avro(
       data: Column,
       schemaId: SchemaGUID,
       clientOptions: java.util.Map[String, String],
       requireExactSchemaMatch: Boolean): Column = {
    if(schemaId == null) {
      throw new NullPointerException("Schema Id cannot be null.")
    }
    new Column(AvroDataToCatalyst(data.expr, schemaId.schemaIdStringValue, SchemaReader.VALUE_NOT_PROVIDED, clientOptions.asScala.toMap, requireExactSchemaMatch))
  }

  def from_avro(
       data: Column,
       schemaId: SchemaGUID,
       clientOptions: java.util.Map[String, String]): Column = {
    from_avro(data, schemaId, clientOptions, false)
  }

  /**
   * Converts a Spark SQL Column into a column containing SR payloads.
   *
   * @param data the data column.
   * @param schemaString The avro schema in JSON string format.
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @return column with SR payloads
   *
   */
  def to_avro(data: Column,
              schemaString: String,
              clientOptions: java.util.Map[java.lang.String, java.lang.String]): Column = {
    new Column(CatalystDataToAvro(data.expr, SchemaReader.VALUE_NOT_PROVIDED, schemaString, clientOptions.asScala.toMap))
  }

  /**
   * Converts a Spark SQL Column into a column containing SR payloads.
   *
   * @param data the data column.
   * @param schemaId The GUID of the expected schema
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @return column with SR payloads
   *
   */
  def to_avro(data: Column,
              schemaId: SchemaGUID,
              clientOptions: java.util.Map[java.lang.String, java.lang.String]): Column = {
    if(schemaId == null) {
      throw new NullPointerException("Schema Id cannot be null.")
    }
    new Column(CatalystDataToAvro(data.expr, schemaId.schemaIdStringValue, SchemaReader.VALUE_NOT_PROVIDED, clientOptions.asScala.toMap))
  }

}

case class SchemaGUID(schemaIdStringValue: String) {}

