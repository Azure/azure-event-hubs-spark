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

import com.azure.data.schemaregistry.implementation.models.ServiceErrorResponseException
import com.azure.data.schemaregistry.models.{SerializationType, SchemaProperties}
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder
import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializerBuilder}
import com.azure.identity.ClientSecretCredentialBuilder
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import scala.util.Try

import functions._

class SchemaReader(
     var schemaId: String,
     var expectedSchemaString: String,
     val options: Map[java.lang.String, java.lang.String]) {

  @transient private lazy val schemaRegistryCredential = new ClientSecretCredentialBuilder()
        .tenantId(options.getOrElse(SCHEMA_REGISTRY_TENANT_ID_KEY, null))
        .clientId(options.getOrElse(SCHEMA_REGISTRY_CLIENT_ID_KEY, null))
        .clientSecret(options.getOrElse(SCHEMA_REGISTRY_CLIENT_SECRET_KEY, null))
        .build()

  @transient private lazy val schemaRegistryAsyncClient = new SchemaRegistryClientBuilder()
        .endpoint(options.getOrElse(SCHEMA_REGISTRY_URL, null))
        .credential(schemaRegistryCredential)
        .buildAsyncClient()

  @transient lazy val serializer =  new SchemaRegistryAvroSerializerBuilder()
    .schemaRegistryAsyncClient(schemaRegistryAsyncClient)
    .schemaGroup(options.getOrElse(SCHEMA_GROUP_KEY, null))
    //.autoRegisterSchema(options.getOrElse(SCHEMA_AUTO_REGISTER_FLAG_KEY, false).asInstanceOf[Boolean])
    .buildSerializer()

  def setSchemaString  = {
    expectedSchemaString = new String(schemaRegistryAsyncClient.getSchema(schemaId).block().getSchema)
  }

  def setSchemaId = {
    val schemaGroup: String = options.getOrElse(SCHEMA_GROUP_KEY, null)
    val schemaName: String = options.getOrElse(SCHEMA_NAME_KEY, null)
    try {
      schemaId = schemaRegistryAsyncClient.getSchemaId(schemaGroup, schemaName, expectedSchemaString, SerializationType.AVRO).block()
    } catch {
      case e: ServiceErrorResponseException => {
        val errorStatusCode = e.getResponse.getStatusCode
        errorStatusCode match {
          case 404 => { // schema not found
            val autoRegistryStr: String = options.getOrElse(SCHEMA_AUTO_REGISTER_FLAG_KEY, "false")
            val autoRegistryFlag: Boolean = Try(autoRegistryStr.toLowerCase.toBoolean).getOrElse(false)
            if(autoRegistryFlag) {
              val schemaProperties = schemaRegistryAsyncClient
                .registerSchema(schemaGroup, schemaName, expectedSchemaString, SerializationType.AVRO)
                .block()
              schemaId = schemaProperties.getSchemaId
            } else {
              throw new SchemaNotFoundException(s"Schema with name=$schemaName and content=$expectedSchemaString does not" +
                s"exist in schemaGroup=$schemaGroup and $SCHEMA_AUTO_REGISTER_FLAG_KEY is set to false. If you want to" +
                s"auto register a new schema make sure to set $SCHEMA_AUTO_REGISTER_FLAG_KEY to true in the properties.")
            }
          }
          case _ => throw e
        }
      }
      case e: Throwable => throw e
    }
  }

  @transient lazy val expectedSchema = new Schema.Parser().parse(expectedSchemaString)
}

object SchemaReader {
  val VALUE_NOT_PROVIDED: String = "NOTHING"

  def createSchemaReader(
        schemaId: String,
        schemaDefinition: String,
        options: Map[java.lang.String, java.lang.String],
        calledByTo_avro: Boolean = false) : SchemaReader = {
    // check for null schema string or schema guid
    if((schemaId == VALUE_NOT_PROVIDED) && (schemaDefinition == null)) {
      throw new NullPointerException("Schema definition cannot be null.")
    }
    else if((schemaDefinition == VALUE_NOT_PROVIDED) && (schemaId == null)) {
      throw new NullPointerException("Schema Id cannot be null.")
    }

    validateOptions(options)
    if(schemaId != VALUE_NOT_PROVIDED) {
      //schema Id is provided
      val schemaReader = new SchemaReader(schemaId, schemaDefinition , options)
      schemaReader.setSchemaString
      schemaReader
    } else {
      // schema definition is provided
      // in case to_avro has been called with the schema itself, the schema group and schema name should be provided to get the schema id
      if(calledByTo_avro) {
        schemaGroupAndNameAreSet(options)
      }
      // ensure schema doesn't have any whitespaces, otherwise getSchemaId API won't work properly!
      val schemaDefinitionWithoutSpaces = schemaDefinition.replaceAll("[\\n\\t ]", "")
      val schemaReader = new SchemaReader(schemaId, schemaDefinitionWithoutSpaces, options)
      if(calledByTo_avro) {
        schemaReader.setSchemaId
      }
      schemaReader
    }
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

  private def schemaGroupAndNameAreSet(options: Map[java.lang.String, java.lang.String]) = {
    if(!options.contains(SCHEMA_GROUP_KEY)) {
      throw new MissingPropertyException(s"schemaRegistryClient requires the schema group to get the schema Guid. " +
        s"Please provide the schema group in the properties, using the $SCHEMA_GROUP_KEY key.")
    }
    if(!options.contains(SCHEMA_NAME_KEY)) {
      throw new MissingPropertyException(s"schemaRegistryClient requires the schema name to get the schema Guid. " +
        s"Please provide the schema name in the properties, using the $SCHEMA_NAME_KEY key.")
    }
  }
}
