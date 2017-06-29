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

package org.apache.spark.eventhubscommon.client

import java.time.Instant

import com.microsoft.azure.eventhubs.{EventHubClient => AzureEventHubClient, PartitionReceiver}
import com.microsoft.azure.servicebus.ConnectionStringBuilder

import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore

private[client] object Common {

  val MINIMUM_PREFETCH_COUNT: Int = 10
  var MAXIMUM_PREFETCH_COUNT: Int = 999
  var MAXIMUM_EVENT_RATE: Int = 0
  val DEFAULT_RECEIVER_EPOCH: Long = -1L

  def configureStartOffset(eventhubsParams: Predef.Map[String, String], offsetStore: OffsetStore):
    (EventHubsOffsetType, String) = {
    // Determine the offset to start receiving data
    val previousOffset = offsetStore.read()
    EventHubsReceiverWrapper.configureStartOffset(previousOffset, eventhubsParams)
  }

  def configureMaxEventRate(userDefinedEventRate: Int): Int = {
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

  def createNewReceiver(
      eventhubsClient: AzureEventHubClient,
      consumerGroup: String,
      partitionId: String,
      offsetType: EventHubsOffsetType,
      currentOffset: String,
      receiverEpoch: Long): PartitionReceiver = {
    offsetType match {
      case EventHubsOffsetTypes.None | EventHubsOffsetTypes.PreviousCheckpoint
           | EventHubsOffsetTypes.InputByteOffset =>
        if (receiverEpoch > DEFAULT_RECEIVER_EPOCH) {
          eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId, currentOffset,
            receiverEpoch)
        } else {
          eventhubsClient.createReceiverSync(consumerGroup, partitionId, currentOffset)
        }
      case EventHubsOffsetTypes.InputTimeOffset =>
        if (receiverEpoch > DEFAULT_RECEIVER_EPOCH) {
          eventhubsClient.createEpochReceiverSync(consumerGroup, partitionId,
            Instant.ofEpochSecond(currentOffset.toLong), receiverEpoch)
        } else {
          eventhubsClient.createReceiverSync(consumerGroup, partitionId,
            Instant.ofEpochSecond(currentOffset.toLong))
        }
    }
  }

  def configureGeneralParameters(eventhubsParams: Predef.Map[String, String]):
    (String, String, Long) = {
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
      AzureEventHubClient.DEFAULT_CONSUMER_GROUP_NAME)
    // Set the epoch if specified
    val receiverEpoch = eventhubsParams.getOrElse("eventhubs.epoch",
      DEFAULT_RECEIVER_EPOCH.toString).toLong
    (connectionString.toString, consumerGroup, receiverEpoch)
  }
}
