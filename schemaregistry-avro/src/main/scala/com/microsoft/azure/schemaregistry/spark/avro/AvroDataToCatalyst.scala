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

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import org.apache.avro.generic.GenericRecord
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types._

case class AvroDataToCatalyst(
     child: Expression,
     schemaId: String,
     options: Map[java.lang.String, java.lang.String])
  extends UnaryExpression with ExpectsInputTypes with Logging{

  override def inputTypes: Seq[BinaryType] = Seq(BinaryType)

  override lazy val dataType: DataType = {
    val dt = SchemaConverters.toSqlType(schemaRegistryConstructor.expectedSchema).dataType;
    dt
  }

  override def nullable: Boolean = true

  @transient private lazy val schemaRegistryConstructor = SchemaRegistryConstructor.init(schemaId, options)

  @transient private lazy val avroConverter = {
    new AvroDeserializer(schemaRegistryConstructor.expectedSchema, dataType)
  }

  override def nullSafeEval(input: Any): Any = {
    val taskId = AvroDataToCatalyst.getTaskId
    val message = new com.azure.core.models.MessageContent()
      .setBodyAsBinaryData(BinaryData.fromBytes(input.asInstanceOf[Array[Byte]]))
      .setContentType(("avro/binary+"+schemaRegistryConstructor.schemaId))

    try {
      logDebug(s"TID $taskId deserializing Message Content with schemaID ${schemaRegistryConstructor.schemaId}")
      val genericRecord = schemaRegistryConstructor.serializer.deserialize(message, TypeReference.createInstance(classOf[GenericRecord]))

      logDebug(s"TID $taskId deserializing Avro data to Catalyst data")
      avroConverter.deserialize(genericRecord)
    } catch {
      case e:Exception =>
      {
        logError(s"TID $taskId Failed to deserialize Avro data to Catalyst data. \nThe schema description is: ${schemaRegistryConstructor.expectedSchema}. \nError Message: ${e.getMessage}. \nCall stack: ${e.getStackTrace}")
        throw new DeserializationException("Failed to deserialize, error: \n" + e)
      }
    }
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }

  override protected def withNewChildInternal(newChild: Expression): AvroDataToCatalyst =
    copy(child = newChild)
}

object AvroDataToCatalyst {
  def getTaskId: Long = {
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.taskAttemptId()
    } else -1
  }
}