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
package org.apache.spark.streaming.eventhubs

import java.time.Instant

import scala.collection.JavaConverters._
import scala.collection.Map

import com.microsoft.azure.eventhubs._
import com.microsoft.azure.eventhubs.{EventHubClient => AzureEventHubClient}
import com.microsoft.azure.servicebus._

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.eventhubs.EventhubsOffsetTypes._
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
class EventHubsClientWrapper extends Serializable with EventHubClient with Logging {

  var eventhubsClient: AzureEventHubClient = _

  // TODO: the design of this class is not simple enough
  // ideally, we shall not require the user to explicitly call createReceiver first
  // and then call receive
  // we shall let the user pass parameters in the constructor directly

  private def configureGeneralParameters(eventhubsParams: Map[String, String]) = {
    if (eventhubsParams.contains("eventhubs.uri") &&
      eventhubsParams.contains("eventhubs.namespace")) {
      throw new IllegalArgumentException(s"Eventhubs URI and namespace cannot both be specified" +
        s" at the same time.")
    }

    val namespaceName = if (eventhubsParams.contains("eventhubs.namespace")) {
      eventhubsParams.get("eventhubs.namespace")
    } else {
      eventhubsParams.get("eventhubs.uri")
    }
    if (namespaceName.isEmpty) {
      throw new IllegalArgumentException(s"Either Eventhubs URI or namespace nust be" +
        s" specified.")
    }
    // TODO: validate inputs
    val evhName = eventhubsParams("eventhubs.name")
    val evhPolicyName = eventhubsParams("eventhubs.policyname")
    val evhPolicyKey = eventhubsParams("eventhubs.policykey")
    val connectionString = new ConnectionStringBuilder(namespaceName.get, evhName, evhPolicyName,
      evhPolicyKey)
    // Set the consumer group if specified.
    val consumerGroup = eventhubsParams.getOrElse("eventhubs.consumergroup",
      EventHubClient.DEFAULT_CONSUMER_GROUP_NAME)
    // Set the epoch if specified
    val receiverEpoch = eventhubsParams.getOrElse("eventhubs.epoch",
      DEFAULT_RECEIVER_EPOCH.toString).toLong
    (connectionString, consumerGroup, receiverEpoch)
  }

  private def configureStartOffset(
      eventhubsParams: Map[String, String], offsetStore: OffsetStore):
      (EventhubsOffsetType, String) = {
    // Determine the offset to start receiving data
    val previousOffset = offsetStore.read()
    if (previousOffset != "-1" && previousOffset != null) {
      (EventhubsOffsetTypes.PreviousCheckpoint, previousOffset)
    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {
      (EventhubsOffsetTypes.InputByteOffset, eventhubsParams("eventhubs.filter.offset"))
    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {
      (EventhubsOffsetTypes.InputTimeOffset, eventhubsParams("eventhubs.filter.enqueuetime"))
    } else {
      (EventhubsOffsetTypes.None, PartitionReceiver.START_OF_STREAM)
    }
  }

  private def configureMaxEventRate(userDefinedEventRate: Int): Int = {
    if (userDefinedEventRate > 0 && userDefinedEventRate < MINIMUM_PREFETCH_COUNT) {
      MAXIMUM_PREFETCH_COUNT = MINIMUM_PREFETCH_COUNT
    } else if (userDefinedEventRate >= MINIMUM_PREFETCH_COUNT &&
      userDefinedEventRate < MAXIMUM_PREFETCH_COUNT) {
      MAXIMUM_PREFETCH_COUNT = userDefinedEventRate + 1
    } else {
      MAXIMUM_EVENT_RATE = MAXIMUM_PREFETCH_COUNT - 1
    }
    MAXIMUM_EVENT_RATE
  }

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String, startOffset: String, maximumEventRate: Int): Unit = {
    val (connectionString, consumerGroup, receiverEpoch) = configureGeneralParameters(
      eventhubsParams)
    val offsetType = EventhubsOffsetTypes.PreviousCheckpoint
    val currentOffset = startOffset
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    createReceiverInternal(connectionString.toString, consumerGroup, partitionId, offsetType,
      currentOffset, receiverEpoch)
  }

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String,
                     offsetStore: OffsetStore,
                     maximumEventRate: Int): Unit = {
    val (connectionString, consumerGroup, receiverEpoch) = configureGeneralParameters(
      eventhubsParams)
    val (offsetType, currentOffset) = configureStartOffset(eventhubsParams, offsetStore)
    logInfo(s"start a receiver for partition $partitionId with the start offset $currentOffset")
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    createReceiverInternal(connectionString.toString, consumerGroup, partitionId, offsetType,
      currentOffset, receiverEpoch)
  }

  private[eventhubs] def createReceiverInternal(
                             connectionString: String,
                             consumerGroup: String,
                             partitionId: String,
                             offsetType: EventhubsOffsetType,
                             currentOffset: String,
                             receiverEpoch: Long): Unit = {
    // Create Eventhubs client
    eventhubsClient = EventHubClient.createFromConnectionStringSync(connectionString)

    eventhubsReceiver = offsetType match {
      case EventhubsOffsetTypes.None | EventhubsOffsetTypes.PreviousCheckpoint
           | EventhubsOffsetTypes.InputByteOffset =>
        if (receiverEpoch > DEFAULT_RECEIVER_EPOCH) {
          eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset,
            receiverEpoch)
        } else {
          eventhubsClient.createReceiverSync(consumerGroup, partitionId, currentOffset)
        }
      case EventhubsOffsetTypes.InputTimeOffset =>
        if (receiverEpoch > DEFAULT_RECEIVER_EPOCH) {
          eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId,
            Instant.ofEpochSecond(currentOffset.toLong), receiverEpoch)
        } else {
          eventhubsClient.createReceiverSync(consumerGroup, partitionId,
            Instant.ofEpochSecond(currentOffset.toLong))
        }
    }
    eventhubsReceiver.setPrefetchCount(MAXIMUM_PREFETCH_COUNT)
  }

  def receive(): Iterable[EventData] = {
    val events = eventhubsReceiver.receive(MAXIMUM_EVENT_RATE).get()
    if (events == null) Iterable.empty else events.asScala
  }

  def receive(expectedEventNum: Int): Iterable[EventData] = {
    val events = eventhubsReceiver.receive(
      math.min(expectedEventNum, eventhubsReceiver.getPrefetchCount)).get()
    if (events == null) Iterable.empty else events.asScala
  }

  override def close(): Unit = {
    if (eventhubsReceiver != null) eventhubsReceiver.closeSync()
    if (eventhubsClient != null) eventhubsClient.closeSync()
  }

  def closeReceiver(): Unit = {
    eventhubsReceiver.closeSync()
  }

  private var eventhubsReceiver: PartitionReceiver = _
  private val MINIMUM_PREFETCH_COUNT: Int = 10
  private var MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
  private val DEFAULT_RECEIVER_EPOCH = -1L

  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubsNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    throw new UnsupportedOperationException("endPointOfPartition is not supported by this client" +
      " yet, please use RestfulEventHubClient")
  }
}

object EventHubsClientWrapper {

  def getEventHubReceiver(
      eventhubsParams: Map[String, String],
      partitionId: Int,
      startOffset: Long,
      maximumEventRate: Int): EventHubsClientWrapper = {

    // TODO: reuse client
    val eventHubClientWrapperInstance = new EventHubsClientWrapper()
    eventHubClientWrapperInstance.createReceiver(eventhubsParams, partitionId.toString,
      startOffset.toString, maximumEventRate)
    eventHubClientWrapperInstance
  }
}
