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

package org.apache.spark.sql.eventhubs

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.client.Client
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ Attribute, Cast, UnsafeProjection }
import org.apache.spark.sql.types.{ BinaryType, StringType }
import org.apache.spark.unsafe.types.UTF8String.IntWrapper

/**
 * Writes out data in a single Spark task, without any concerns about how
 * to commit or abort tasks. Exceptions thrown by the implementation of this
 * class will automatically trigger task aborts.
 */
private[eventhubs] class EventHubsWriteTask(parameters: Map[String, String],
                                            inputSchema: Seq[Attribute],
                                            clientFactory: (EventHubsConf) => Client)
    extends EventHubsRowWriter(inputSchema) {

  private var sender: Client = _
  private val ehConf = EventHubsConf.toConf(parameters)

  /**
   * Writers data out to EventHubs
   *
   * @param iterator contains all rows to be written to EventHubs
   */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    sender = clientFactory(ehConf)
    while (iterator.hasNext) {
      val currentRow = iterator.next
      sendRow(currentRow, sender)
    }
  }

  def close(): Unit = {
    if (sender != null) {
      sender.close()
      sender = null
    }
  }
}

private[eventhubs] abstract class EventHubsRowWriter(inputSchema: Seq[Attribute]) {

  protected val projection: UnsafeProjection = createProjection

  /**
   * Send the specified row to EventHubs.
   */
  protected def sendRow(
      row: InternalRow,
      sender: Client
  ): Unit = {
    val projectedRow = projection(row)
    val body = projectedRow.getBinary(0)
    val partitionKey = projectedRow.getUTF8String(1)
    val partitionId = projectedRow.getUTF8String(2)

    require(partitionId != null || partitionKey != null,
            "A partitionId and partitionKey have been set. This is not allowed.")

    val event = EventData.create(body)

    if (partitionKey != null) {
      sender.send(event, partitionKey.toString)
    } else if (partitionId != null) {
      val wrapper = new IntWrapper
      if (partitionId.toInt(wrapper)) {
        sender.createPartitionSender(wrapper.value)
        sender.send(event, wrapper.value)
      } else {
        throw new IllegalStateException(
          s"partitionId '$partitionId' could not be parsed to an int.")
      }
    } else {
      sender.send(event)
    }
  }

  private def createProjection = {
    val bodyExpression = inputSchema
      .find(_.name == EventHubsWriter.BodyAttributeName)
      .getOrElse(throw new IllegalStateException(
        s"Required attribute '${EventHubsWriter.BodyAttributeName}' not found."))
    bodyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(
          s"${EventHubsWriter.BodyAttributeName} attribute unsupported type $t")
    }

    val partitionKeyExpression =
      inputSchema.find(_.name == EventHubsWriter.PartitionKeyAttributeName).orNull
    partitionKeyExpression.dataType match {
      case null | StringType => // good
      case t =>
        throw new IllegalStateException(
          s"${EventHubsWriter.PartitionKeyAttributeName} attribute unsupported type $t"
        )
    }

    val partitionIdExpression =
      inputSchema.find(_.name == EventHubsWriter.PartitionIdAttributeName).orNull
    partitionIdExpression.dataType match {
      case null | StringType => // good
      case t =>
        throw new IllegalStateException(
          s"${EventHubsWriter.PartitionKeyAttributeName} attribute unsupported type $t"
        )
    }

    UnsafeProjection.create(Seq(Cast(bodyExpression, BinaryType),
                                Cast(partitionKeyExpression, StringType),
                                Cast(partitionIdExpression, StringType)),
                            inputSchema)
  }
}
