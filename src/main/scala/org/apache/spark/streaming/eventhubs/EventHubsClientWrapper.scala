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
import com.microsoft.azure.servicebus._

import org.apache.spark.streaming.eventhubs.EventhubsOffsetTypes._

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
class EventHubsClientWrapper extends Serializable {

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String,
                     offsetStore: OffsetStore,
                     maximumEventRate: Int): Unit = {

    // Create Eventhubs connection string either from namespace (with default URI) or from specified
    // URI

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

    // Determine the offset to start receiving data
    var offsetType = EventhubsOffsetTypes.None
    var currentOffset: String = PartitionReceiver.START_OF_STREAM
    val previousOffset = offsetStore.read()
    if (previousOffset != "-1" && previousOffset != null) {
      offsetType = EventhubsOffsetTypes.PreviousCheckpoint
      currentOffset = previousOffset
    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {
      offsetType = EventhubsOffsetTypes.InputByteOffset
      currentOffset = eventhubsParams("eventhubs.filter.offset")
    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {
      offsetType = EventhubsOffsetTypes.InputTimeOffset
      currentOffset = eventhubsParams("eventhubs.filter.enqueuetime")
    }

    MAXIMUM_EVENT_RATE = maximumEventRate
    if (maximumEventRate > 0 && maximumEventRate < MINIMUM_PREFETCH_COUNT) {
      MAXIMUM_PREFETCH_COUNT = MINIMUM_PREFETCH_COUNT
    } else if (maximumEventRate >= MINIMUM_PREFETCH_COUNT &&
      maximumEventRate < MAXIMUM_PREFETCH_COUNT) {
      MAXIMUM_PREFETCH_COUNT = maximumEventRate + 1
    } else {
      MAXIMUM_EVENT_RATE = MAXIMUM_PREFETCH_COUNT - 1
    }
    createReceiverInternal(connectionString.toString, consumerGroup, partitionId, offsetType,
      currentOffset, receiverEpoch)
  }

  private[eventhubs]
  def createReceiverInternal(connectionString: String,
                             consumerGroup: String,
                             partitionId: String,
                             offsetType: EventhubsOffsetType,
                             currentOffset: String,
                             receiverEpoch: Long): Unit = {

    // Create Eventhubs client
    val eventhubsClient = EventHubClient.createFromConnectionStringSync(connectionString)

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

  def close(): Unit = if (eventhubsReceiver != null) eventhubsReceiver.close()

  private var eventhubsReceiver: PartitionReceiver = _
  private val MINIMUM_PREFETCH_COUNT: Int = 10
  private var MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
  private val DEFAULT_RECEIVER_EPOCH = -1L
}
