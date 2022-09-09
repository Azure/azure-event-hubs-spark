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

import org.apache.spark.sql.Column

import scala.collection.JavaConverters._

/***
 * Scala object containing utility methods for deserialization with Azure Schema Registry and Spark SQL
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

  /***
   * Converts Spark SQL Column containing SR payloads into its corresponding catalyst value.
   * This method uses schema GUID to get the schema description which is used to serialize the data.
   * If schemaId is provided, it must be the Schema GUID of the actual schema used to serialize the data.
   *
   * @param data column with SR payloads
   * @param schemaId The GUID of the expected schema.
   * @param clientOptions map of configuration properties
   */
  def from_avro(
       data: Column,
       schemaId: SchemaGUID,
       clientOptions: java.util.Map[String, String]): Column = {
    if(schemaId == null) {
      throw new NullPointerException("Schema Id cannot be null.")
    }
    new Column(AvroDataToCatalyst(data.expr, schemaId.schemaIdStringValue, clientOptions.asScala.toMap))
  }

}

case class SchemaGUID(schemaIdStringValue: String) {}

