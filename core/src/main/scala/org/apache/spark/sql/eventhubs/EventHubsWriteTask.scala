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
import org.apache.spark.eventhubs.{ EventHubsConf, EventHubsUtils }
import org.apache.spark.eventhubs.client.Client
import org.apache.spark.eventhubs.utils.MetricPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Cast,
  Literal,
  UnsafeMapData,
  UnsafeProjection
}
import org.apache.spark.unsafe.types.UTF8String.IntWrapper
import org.apache.spark.sql.types.{ BinaryType, MapType, StringType }
import org.apache.spark.unsafe.types.UTF8String

/**
 * Writes out data in a single Spark task, without any concerns about how
 * to commit or abort tasks. Exceptions thrown by the implementation of this
 * class will automatically trigger task aborts.
 */
private[eventhubs] class EventHubsWriteTask(parameters: Map[String, String],
                                            inputSchema: Seq[Attribute])
    extends EventHubsRowWriter(inputSchema)
    with Logging {

  private val ehConf = EventHubsConf.toConf(parameters)
  private var sender: Client = _
  private lazy val metricPlugin: Option[MetricPlugin] = ehConf.metricPlugin()
  var totalMessageSizeInBytes = 0
  var totalMessageCount = 0
  var writerOpenTime = 0L

  /**
   * Writers data out to EventHubs
   *
   * @param iterator contains all rows to be written to EventHubs
   */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    sender = EventHubsSourceProvider.clientFactory(parameters)(ehConf)
    writerOpenTime = System.currentTimeMillis()
    while (iterator.hasNext) {
      val currentRow = iterator.next
      totalMessageSizeInBytes += sendRow(currentRow, sender)
      totalMessageCount += 1
    }
  }

  def close(): Unit = {
    log.info(s"close is called. ${EventHubsUtils.getTaskContextSlim}")

    var success = false
    if (sender != null) {
      try {
        sender.close()
        success = true
      } catch {
        case e: Exception =>
          log.warn(s"an error occurred. eventhub name = ${ehConf.name}, error = ${e.getMessage}")
          throw e
      }
      sender = null
    }

    metricPlugin.foreach(
      _.onSendMetric(EventHubsUtils.getTaskContextSlim,
                     ehConf.name,
                     totalMessageCount,
                     totalMessageSizeInBytes,
                     System.currentTimeMillis() - writerOpenTime,
                     isSuccess = success))

  }
}

private[eventhubs] abstract class EventHubsRowWriter(inputSchema: Seq[Attribute]) {

  protected val projection: UnsafeProjection = createProjection

  private def toPartitionKey(partitionKey: UTF8String): Option[String] = {
    if (partitionKey == null) {
      None
    } else {
      Some(partitionKey.toString)
    }
  }

  private def toPartitionId(partitionId: UTF8String): Option[Int] = {
    if (partitionId == null) {
      None
    } else {
      val wrapper = new IntWrapper
      assert(partitionId.toInt(wrapper))
      Some(wrapper.value)
    }
  }

  private def toProperties(unsafeMap: UnsafeMapData): Option[Map[String, String]] = {
    if (unsafeMap == null) {
      None
    } else {
      val keys = unsafeMap.keyArray()
      val values = unsafeMap.valueArray()
      Some((0 until keys.numElements).map { i =>
        if (keys.isNullAt(i)) throw new IllegalStateException("Properties cannot have a null key")
        if (values.isNullAt(i))
          throw new IllegalStateException("Properties cannot have a null value")
        keys.getUTF8String(i).toString -> values.getUTF8String(i).toString
      }.toMap)
    }
  }

  /**
   * Send the specified row to EventHubs.
   */
  protected def sendRow(
      row: InternalRow,
      sender: Client
  ): Int = {
    val projectedRow = projection(row)
    val body = projectedRow.getBinary(0)
    val partitionKey = toPartitionKey(projectedRow.getUTF8String(1))
    val partitionId = toPartitionId(projectedRow.getUTF8String(2))
    val properties = toProperties(projectedRow.getMap(3))

    require(
      partitionId.isEmpty || partitionKey.isEmpty,
      s"Both a partitionKey (${partitionKey.get}) and partition (${partitionId.get}) have been detected. Both can not be set."
    )

    val event = EventData.create(body)
    sender.send(event, partitionId, partitionKey, properties)
    event.getBytes.length
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
      inputSchema
        .find(_.name == EventHubsWriter.PartitionKeyAttributeName)
        .getOrElse(Literal(null, StringType))

    partitionKeyExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(
          s"${EventHubsWriter.PartitionKeyAttributeName} attribute unsupported type $t"
        )
    }

    val partitionIdExpression =
      inputSchema
        .find(attr =>
          (attr.name == EventHubsWriter.PartitionIdAttributeNameAlias) || (attr.name == EventHubsWriter.PartitionIdAttributeName))
        .getOrElse(Literal(null, StringType))

    partitionIdExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(
          s"${EventHubsWriter.PartitionIdAttributeNameAlias} attribute unsupported type $t"
        )
    }

    val propertiesExpression =
      inputSchema
        .find(_.name == EventHubsWriter.PropertiesAttributeName)
        .getOrElse(Literal(null, MapType(StringType, StringType)))

    propertiesExpression.dataType match {
      case MapType(StringType, StringType, true)  => // good
      case MapType(StringType, StringType, false) => // good
      case t =>
        throw new IllegalStateException(
          s"${EventHubsWriter.PropertiesAttributeName} attribute unsupported type $t"
        )
    }

    UnsafeProjection.create(
      Seq(
        Cast(bodyExpression, BinaryType),
        Cast(partitionKeyExpression, StringType),
        Cast(partitionIdExpression, StringType),
        Cast(propertiesExpression, MapType(StringType, StringType))
      ),
      inputSchema
    )
  }
}
