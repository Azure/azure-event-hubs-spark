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

import scala.collection.JavaConverters._

import com.microsoft.azure.eventhubs.{EventHubClient => AzureEventHubClient, _}
import EventHubsOffsetTypes.EventHubsOffsetType

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsReceiverWrapper extends Logging {

  import Common._

  var eventhubsClient: AzureEventHubClient = _
  private var eventhubsReceiver: PartitionReceiver = _

  /**
   * create a client without initializing receivers
   *
   * the major purpose of this API is for creating AMQP management client
   */
  def createClient(eventhubsParams: Map[String, String]): AzureEventHubClient = {
    val (connectionString, _, _) = configureGeneralParameters(eventhubsParams)
    eventhubsClient = AzureEventHubClient.createFromConnectionStringSync(connectionString.toString)
    eventhubsClient
  }

  def createReceiver(
      eventhubsParams: Predef.Map[String, String],
      partitionId: String,
      startOffset: String,
      offsetType: EventHubsOffsetType,
      maximumEventRate: Int): Unit = {
    val (connectionString, consumerGroup, receiverEpoch) = configureGeneralParameters(
      eventhubsParams)
    val currentOffset = startOffset
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    createReceiverInternal(connectionString.toString, consumerGroup, partitionId, offsetType,
      currentOffset, receiverEpoch)
  }

  def createReceiver(
      eventhubsParams: Map[String, String],
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

  private[spark] def createReceiverInternal(
      connectionString: String,
      consumerGroup: String,
      partitionId: String,
      offsetType: EventHubsOffsetType,
      currentOffset: String,
      receiverEpoch: Long): Unit = {
    // Create Eventhubs client
    eventhubsClient = AzureEventHubClient.createFromConnectionStringSync(connectionString)
    eventhubsReceiver = createNewReceiver(eventhubsClient,
      consumerGroup, partitionId, offsetType, currentOffset, receiverEpoch)
    eventhubsReceiver.setPrefetchCount(MAXIMUM_PREFETCH_COUNT)
  }

  def receive(): Iterable[EventData] = {
    val events = eventhubsReceiver.receive(MAXIMUM_EVENT_RATE).get()
    if (events == null) Iterable.empty else events.asScala
  }

  /**
   * starting from EventHubs client 0.13.1, returning a null from receiver means that there is
   * no message in server end
   */
  def receive(expectedEventNum: Int): Iterable[EventData] = {
    val events = eventhubsReceiver.receive(
      math.min(expectedEventNum, eventhubsReceiver.getPrefetchCount)).get()
    if (events != null) events.asScala else null
  }

  def close(): Unit = {
    if (eventhubsReceiver != null) eventhubsReceiver.closeSync()
    if (eventhubsClient != null) eventhubsClient.closeSync()
  }

  def closeReceiver(): Unit = {
    eventhubsReceiver.closeSync()
  }
}

private[spark] object EventHubsReceiverWrapper extends Logging {

  private[eventhubscommon] def configureStartOffset(
      previousOffset: String,
      eventhubsParams: Predef.Map[String, String]): (EventHubsOffsetType, String) = {
    if (previousOffset != "-1" && previousOffset != null) {
      (EventHubsOffsetTypes.PreviousCheckpoint, previousOffset)
    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {
      (EventHubsOffsetTypes.InputByteOffset, eventhubsParams("eventhubs.filter.offset"))
    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {
      (EventHubsOffsetTypes.InputTimeOffset, eventhubsParams("eventhubs.filter.enqueuetime"))
    } else {
      (EventHubsOffsetTypes.None, PartitionReceiver.START_OF_STREAM)
    }
  }

  def getEventHubsClient(eventhubsParams: Map[String, String]): AzureEventHubClient = {
    new EventHubsReceiverWrapper().createClient(eventhubsParams)
  }

  def getEventHubReceiver(
      eventhubsParams: Predef.Map[String, String],
      partitionId: Int,
      startOffset: Long,
      offsetType: EventHubsOffsetType,
      maximumEventRate: Int): EventHubsReceiverWrapper = {

    // TODO: reuse client
    val eventHubClientWrapperInstance = new EventHubsReceiverWrapper()
    eventHubClientWrapperInstance.createReceiver(eventhubsParams, partitionId.toString,
      startOffset.toString, offsetType, maximumEventRate)
    eventHubClientWrapperInstance
  }
}
