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

import com.azure.data.schemaregistry.SchemaRegistryClientBuilder
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder
import com.azure.identity.ClientSecretCredentialBuilder
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import org.apache.avro.Schema
import org.apache.spark.TaskContext
import sun.java2d.marlin.MarlinUtils.logInfo

class SchemaRegistryConstructor(
     var schemaId: String,
     val options: Map[java.lang.String, java.lang.String]){

  @transient private lazy val schemaRegistryCredential = new ClientSecretCredentialBuilder()
        .tenantId(options.getOrElse(SCHEMA_REGISTRY_TENANT_ID_KEY, null))
        .clientId(options.getOrElse(SCHEMA_REGISTRY_CLIENT_ID_KEY, null))
        .clientSecret(options.getOrElse(SCHEMA_REGISTRY_CLIENT_SECRET_KEY, null))
        .build()

  @transient private lazy val schemaRegistryAsyncClient = new SchemaRegistryClientBuilder()
        .fullyQualifiedNamespace(options.getOrElse(SCHEMA_REGISTRY_URL, null))
        .credential(schemaRegistryCredential)
        .buildAsyncClient()

  @transient lazy val serializer =  new SchemaRegistryApacheAvroSerializerBuilder()
        .schemaRegistryClient(schemaRegistryAsyncClient)
        .buildSerializer()

  var expectedSchemaString: String = "NOTHING"

  def setSchemaString  = {
    logInfo(s"Setting up schema description using schemaID $schemaId")
    val schemaRegistrySchema = schemaRegistryAsyncClient.getSchema(schemaId).block()
    logInfo(s"The schema description is ${schemaRegistrySchema.getDefinition}")
    expectedSchemaString = schemaRegistrySchema.getDefinition
  }

  @transient lazy val expectedSchema = new Schema.Parser().parse(expectedSchemaString)
}

object SchemaRegistryConstructor {
  val VALUE_NOT_PROVIDED: String = "NOTHING"

  def init(
        schemaId: String,
        options: Map[java.lang.String, java.lang.String]) : SchemaRegistryConstructor = {

    // check for null schema guid
    if(schemaId == null){
      throw new NullPointerException("Schema Id cannot be null.")
    }

    validateOptions(options)
    val schemaRegistryConstructor = new SchemaRegistryConstructor(schemaId, options)
    schemaRegistryConstructor.setSchemaString

    val taskId = SchemaRegistryConstructor.getTaskId
    logInfo(s"TID $taskId creates schemaRegistryConstructor using schema registry URL: ${options.getOrElse(SCHEMA_REGISTRY_URL, null)}")
    schemaRegistryConstructor
  }

  def getTaskId: Long = {
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.taskAttemptId()
    } else -1
  }

  private def validateOptions(
       options: Map[java.lang.String, java.lang.String]) = {
    // tenant id, client id, client secret and endpoint url should be present in all cases
    if(!options.contains(SCHEMA_REGISTRY_TENANT_ID_KEY)) {
      throw new MissingPropertyException(s"schemaRegistryCredential requires the tenant id. Please provide the " +
        s"tenant id in the properties, using the $SCHEMA_REGISTRY_TENANT_ID_KEY key.")
    }
    if(!options.contains(SCHEMA_REGISTRY_CLIENT_ID_KEY)) {
      throw new MissingPropertyException(s"schemaRegistryCredential requires the client id. Please provide the " +
        s"client id in the properties, using the $SCHEMA_REGISTRY_CLIENT_ID_KEY key.")
    }
    if(!options.contains(SCHEMA_REGISTRY_CLIENT_SECRET_KEY)) {
      throw new MissingPropertyException(s"schemaRegistryCredential requires the client secret. Please provide the " +
        s"client secret in the properties, using the $SCHEMA_REGISTRY_CLIENT_SECRET_KEY key.")
    }
    if(!options.contains(SCHEMA_REGISTRY_URL)) {
      throw new MissingPropertyException(s"schemaRegistryClient requires the endpoint url. Please provide the " +
        s"endpoint url in the properties, using the $SCHEMA_REGISTRY_URL key.")
    }
  }
}
