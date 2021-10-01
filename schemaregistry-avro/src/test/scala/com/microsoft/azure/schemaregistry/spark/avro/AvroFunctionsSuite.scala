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

import java.util
import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AvroFunctionsSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("from_avro do not handle null column") {
    try {
      functions.from_avro(null, "schema_id", null)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("from_avro do not handle null client options") {
    try {
      functions.from_avro(new Column("empty"), "schema_id", null)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }


  test("schema Id cannot be Null") {
    try {
      val configMap: util.Map[String, String] = new util.HashMap[String, String]()
      val schemaReader = SchemaReader.createSchemaReader(null, SchemaReader.VALUE_NOT_PROVIDED, configMap.asScala.toMap)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("schema Definition cannot be Null") {
    try {
      val configMap: util.Map[String, String] = new util.HashMap[String, String]()
      val schemaReader = SchemaReader.createSchemaReader(SchemaReader.VALUE_NOT_PROVIDED, null, configMap.asScala.toMap)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("from_avro invalid client options -- missing tenant_id") {
    // tenant_id, client_id and client_secret must be provided
    val configMap: util.Map[String, String] = new util.HashMap[String, String]()
    configMap.put(functions.SCHEMA_REGISTRY_URL, "https://namespace.servicebus.windows.net")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_ID_KEY, "client_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    val caughtEx = intercept[MissingPropertyException] {
      val schemaReader = SchemaReader.createSchemaReader("schema_id", SchemaReader.VALUE_NOT_PROVIDED, configMap.asScala.toMap)
      //functions.from_avro(new Column("empty"), "schema_id", configMap)
    }
    assert(caughtEx.getMessage == "schemaRegistryCredential requires the tenant id. Please provide the tenant id in the properties, using the schema.registry.tenant.id key.")
  }

  test("from_avro invalid client options -- missing endpoint url") {
    // tenant_id, client_id and client_secret must be provided
    val configMap = new util.HashMap[String, String]()
    configMap.put(functions.SCHEMA_REGISTRY_TENANT_ID_KEY, "tenant_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_ID_KEY, "client_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    val caughtEx = intercept[MissingPropertyException] {
      val schemaReader = SchemaReader.createSchemaReader("schema_id", SchemaReader.VALUE_NOT_PROVIDED, configMap.asScala.toMap)
      //functions.from_avro(new Column("empty"), "schema_id", configMap)
    }
    assert(caughtEx.getMessage == "schemaRegistryClient requires the endpoint url. Please provide the endpoint url in the properties, using the schema.registry.url key.")
  }

  test("to_avro do not handle null column") {
    try {
      functions.to_avro(null, "schema_id", null)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("to_avro do not handle null client options") {
    try {
      functions.to_avro(new Column("empty"), "schema_id", null)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("to_avro invalid client options -- missing client_id") {
    // tenant_id, client_id and client_secret must be provided
    val configMap = new util.HashMap[String, String]()
    configMap.put(functions.SCHEMA_REGISTRY_URL, "https://namespace.servicebus.windows.net")
    configMap.put(functions.SCHEMA_REGISTRY_TENANT_ID_KEY, "tenant_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    val caughtEx = intercept[MissingPropertyException] {
      val schemaReader = SchemaReader.createSchemaReader("schema_id", SchemaReader.VALUE_NOT_PROVIDED, configMap.asScala.toMap, true)
      //functions.to_avro(new Column("empty"), "schema_id", configMap)
    }
    assert(caughtEx.getMessage == "schemaRegistryCredential requires the client id. Please provide the client id in the properties, using the schema.registry.client.id key.")
  }

  test("to_avro with schema content requires the schema group and schema name in client options -- missing schema group") {
    // tenant_id, client_id and client_secret must be provided
    val configMap = new util.HashMap[String, String]()
    configMap.put(functions.SCHEMA_REGISTRY_URL, "https://namespace.servicebus.windows.net")
    configMap.put(functions.SCHEMA_REGISTRY_TENANT_ID_KEY, "tenant_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_ID_KEY, "client_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    configMap.put(functions.SCHEMA_NAME_KEY, "schema_name")
    val caughtEx = intercept[MissingPropertyException] {
      val schemaReader = SchemaReader.createSchemaReader(SchemaReader.VALUE_NOT_PROVIDED, "schema_content", configMap.asScala.toMap, true)
      //functions.to_avro(new Column("empty"), "schema_content", configMap, false)
    }
    assert(caughtEx.getMessage == "schemaRegistryClient requires the schema group to get the schema Guid. " +
    s"Please provide the schema group in the properties, using the schema.group key.")
  }

  test("to_avro with schema content requires the schema group and schema name in client options -- missing schema name") {
    // tenant_id, client_id and client_secret must be provided
    val configMap = new util.HashMap[String, String]()
    configMap.put(functions.SCHEMA_REGISTRY_URL, "https://namespace.servicebus.windows.net")
    configMap.put(functions.SCHEMA_REGISTRY_TENANT_ID_KEY, "tenant_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_ID_KEY, "client_id")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    configMap.put(functions.SCHEMA_REGISTRY_CLIENT_SECRET_KEY, "client_secret")
    configMap.put(functions.SCHEMA_GROUP_KEY, "schema_group")
    val caughtEx = intercept[MissingPropertyException] {
      val schemaReader = SchemaReader.createSchemaReader(SchemaReader.VALUE_NOT_PROVIDED, "schema_content", configMap.asScala.toMap, true)
      //functions.to_avro(new Column("empty"), "schema_content", configMap, false)
    }
    assert(caughtEx.getMessage == "schemaRegistryClient requires the schema name to get the schema Guid. " +
      s"Please provide the schema name in the properties, using the schema.name key.")
  }

}
