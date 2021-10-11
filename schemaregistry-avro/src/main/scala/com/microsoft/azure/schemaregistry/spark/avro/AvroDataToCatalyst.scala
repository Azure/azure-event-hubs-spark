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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.azure.core.util.serializer.TypeReference
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder
import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializerBuilder}
import com.azure.identity.ClientSecretCredentialBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.util.control.NonFatal
import scala.util.Try
import org.apache.spark.internal.Logging

case class AvroDataToCatalyst(
     child: Expression,
     schemaId: String,
     schemaDefinition: String,
     options: Map[java.lang.String, java.lang.String])
  extends UnaryExpression with ExpectsInputTypes with Logging {

  override def inputTypes: Seq[BinaryType] = Seq(BinaryType)

  override lazy val dataType: DataType = {
    val dt = SchemaConverters.toSqlType(schemaReader.expectedSchema).dataType;
    dt
  }

  override def nullable: Boolean = true

  @transient private lazy val schemaReader = SchemaReader.createSchemaReader(schemaId, schemaDefinition, options)

  @transient private lazy val avroConverter = {
    new AvroDeserializer(schemaReader.expectedSchema, dataType)
  }

  @transient private lazy val requireExactSchemaMatch: Boolean = {
    val requiredExactMatchStr: String = options.getOrElse(functions.SCHEMA_EXACT_MATCH_REQUIRED, "false")
    Try(requiredExactMatchStr.toLowerCase.toBoolean).getOrElse(false)
  }

  @transient private lazy val parseMode: ParseMode = {
    val modeStr = schemaReader.options.getOrElse(functions.SCHEMA_PARSE_MODE, FailFastMode.name)
    val mode = ParseMode.fromString(modeStr)
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw new IllegalArgumentException(mode + " parse mode not supported.")
    }
    mode
  }

  @transient private lazy val nullResultRow: Any = dataType match {
    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      for(i <- 0 until st.length) {
        resultRow.setNullAt(i)
      }
      resultRow

    case _ =>
      null
  }

  override def nullSafeEval(input: Any): Any = {
  //  try {
      val binary = new ByteArrayInputStream(input.asInstanceOf[Array[Byte]])
      // compare schema version and datatype version
      val genericRecord = schemaReader.serializer.deserialize(binary, TypeReference.createInstance(classOf[GenericRecord]))

      if (requireExactSchemaMatch) {
        if (!schemaReader.expectedSchema.equals(genericRecord.getSchema)) {
          throw new IncompatibleSchemaException(s"Schema not exact match, payload schema did not match expected schema.  Payload schema: ${genericRecord.getSchema}")
        }
      }

    try {
      avroConverter.deserialize(genericRecord)
  } catch {
      case NonFatal(e) => parseMode match {
        case PermissiveMode => //nullResultRow
          throw new Exception(s"nave Permissive --> error message is $e")
        case FailFastMode =>
          throw new Exception("Malformed records are detected in record parsing. " +
            s"Current parse Mode: ${FailFastMode.name}. To process malformed records as null " +
            "result, try setting the option 'mode' as 'PERMISSIVE'.", e)
        case _ =>
          throw new Exception(s"Unknown parse mode: ${parseMode.name}")
      }
    }
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }

  /*
  private def getSchemaIdFromPayload(input: Any): String = {
    logInfo(s" nave in getSchemaIdFromPayload input = $input.")
    val inputBytes = input.asInstanceOf[Array[Byte]]
    val schemaIdBytes = inputBytes.slice(4,36)
    val schemaId = new String(schemaIdBytes, StandardCharsets.UTF_8)
    schemaId
  }
*/

}