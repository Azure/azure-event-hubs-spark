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

import java.io.ByteArrayOutputStream

import com.azure.data.schemaregistry.SchemaRegistryClientBuilder
import com.azure.data.schemaregistry.avro.SchemaRegistryAvroSerializerBuilder
import com.azure.identity.ClientSecretCredentialBuilder

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BinaryType, DataType}

case class CatalystDataToAvro(
     child: Expression,
     schemaId: String,
     schemaDefinition: String,
     options: Map[java.lang.String, java.lang.String])
  extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val schemaReader = SchemaReader.createSchemaReader(schemaId, schemaDefinition, options, true)

  @transient private lazy val avroConverter =
    new AvroSerializer(child.dataType, schemaReader.expectedSchema, child.nullable)

  @transient private lazy val writer =
    new GenericDatumWriter[Any](schemaReader.expectedSchema)

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  override def nullSafeEval(input: Any): Any = {
    out.reset()
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = avroConverter.serialize(input)
    val prefixBytes = Array[Byte](0, 0, 0, 0)
    val payloadPrefixBytes = prefixBytes ++ schemaReader.schemaId.getBytes()

    writer.write(avroData, encoder)
    encoder.flush()

    val payloadOut = payloadPrefixBytes ++ out.toByteArray
    payloadOut
  }

  override def prettyName: String = "to_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }

}
