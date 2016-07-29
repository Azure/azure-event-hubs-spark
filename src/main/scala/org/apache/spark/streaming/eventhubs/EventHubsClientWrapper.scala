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

import com.microsoft.azure.eventhubs._
import com.microsoft.azure.servicebus._
import org.apache.spark.streaming.eventhubs.EventhubsOffsetType._

import scala.collection.Map
import collection.JavaConverters._

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
class EventHubsClientWrapper extends Serializable {

  def createReceiver(eventhubsParams: Map[String, String],
                     partitionId: String,
                     offsetStore: OffsetStore,
                     maximumEventRate: Int
                    ): Unit = {

    //Create Eventhubs connection string

    val connectionString: ConnectionStringBuilder = new ConnectionStringBuilder(eventhubsParams("eventhubs.namespace"),
      eventhubsParams("eventhubs.name"), eventhubsParams("eventhubs.policyname"),
      eventhubsParams("eventhubs.policykey"))

    //Set the consumer group if specified.

    val consumerGroup: String = if( eventhubsParams.contains("eventhubs.consumergroup"))
      eventhubsParams("eventhubs.consumergroup")
    else EventHubClient.DEFAULT_CONSUMER_GROUP_NAME

    //Set the epoch if specified

    val receiverEpoch: Int = if (eventhubsParams.contains("eventhubs.epoch"))
      eventhubsParams("eventhubs.epoch").toInt
    else 1

    //Determine the offset to start receiving data

    var offsetType = EventhubsOffsetType.None
    var currentOffset: String = PartitionReceiver.START_OF_STREAM

    val previousOffset = offsetStore.read()

    if(previousOffset != "-1" && previousOffset != null) {

      offsetType = EventhubsOffsetType.PreviousCheckpoint
      currentOffset = previousOffset

    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {

      offsetType = EventhubsOffsetType.InputByteOffset
      currentOffset = eventhubsParams("eventhubs.filter.offset")

    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {

      offsetType = EventhubsOffsetType.InputTimeOffset
      currentOffset = eventhubsParams("eventhubs.filter.enqueuetime")
    }

    createReceiverInternal(connectionString.toString(), consumerGroup, partitionId, offsetType,
      currentOffset, receiverEpoch)

    MAXIMUM_EVENT_RATE = if (maximumEventRate <= 0) MAXIMUM_PREFETCH_COUNT else maximumEventRate
  }

  private[eventhubs]
  def createReceiverInternal(connectionString: String,
                             consumerGroup: String,
                             partitionId: String,
                             offsetType: EventhubsOffsetType,
                             currentOffset: String,
                             receiverEpoch: Int): Unit = {

    //Create Eventhubs client
    val eventhubsClient: EventHubClient = EventHubClient.createFromConnectionString(connectionString).get()

    //Create Eventhubs receiver  based on the offset type and specification
    offsetType match  {

      case EventhubsOffsetType.None => eventhubsReceiver = eventhubsClient.createEpochReceiver(consumerGroup,
        partitionId, currentOffset, false, receiverEpoch).get()

      case EventhubsOffsetType.PreviousCheckpoint => eventhubsClient.createEpochReceiver(consumerGroup,
        partitionId, currentOffset, false, receiverEpoch).get()

      case EventhubsOffsetType.InputByteOffset => eventhubsClient.createEpochReceiver(consumerGroup,
        partitionId, currentOffset, false, receiverEpoch).get()

      case EventhubsOffsetType.InputTimeOffset => eventhubsReceiver = eventhubsClient.createEpochReceiver(consumerGroup,
          partitionId, Instant.ofEpochSecond(currentOffset.toLong), receiverEpoch).get()
    }

    eventhubsReceiver.setPrefetchCount(MAXIMUM_EVENT_RATE)
  }

  def receive(): Iterable[EventData] = {

    val receivedEvents: Iterable[EventData] = eventhubsReceiver.receive(MAXIMUM_EVENT_RATE).get().asScala

    if (receivedEvents != null) List.empty[EventData]
    else receivedEvents
  }

  def close(): Unit = {

    if(eventhubsReceiver != null) {

      eventhubsReceiver.close()
    }
  }

  private var eventhubsReceiver: PartitionReceiver = null
  private val MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
}
