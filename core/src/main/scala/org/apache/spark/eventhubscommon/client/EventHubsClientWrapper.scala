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

import scala.collection.JavaConverters._
import EventHubsOffsetTypes.EventHubsOffsetType
import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClientWrapper(
    ehParams: Map[String, Map[String, String]]
) extends Serializable
    with Client
    with Logging {

  private val MINIMUM_PREFETCH_COUNT: Int = 10
  private var MAXIMUM_PREFETCH_COUNT: Int = 999
  private var MAXIMUM_EVENT_RATE: Int = 0
  private val DEFAULT_RECEIVER_EPOCH = -1L

  private val ehNamespace = ehParams("eventhubs.namespace").toString
  private val ehName = ehParams("eventhubs.name").toString
  private val ehPolicyName = ehParams("eventhubs.policyname").toString
  private val ehPolicy = ehParams("eventhubs.policykey").toString

  private val connectionString =
    new ConnectionStringBuilder(ehNamespace, ehName, ehPolicyName, ehPolicy).toString
  private val consumerGroup = ehParams
    .getOrElse("eventhubs.consumergroup", EventHubClient.DEFAULT_CONSUMER_GROUP_NAME)
    .toString
  private val receiverEpoch = ehParams
    .getOrElse("eventhubs.epoch", DEFAULT_RECEIVER_EPOCH.toString)
    .toString
    .toLong

  var eventhubsClient: EventHubClient = _
  private var eventhubsReceiver: PartitionReceiver = _

  private def configureStartOffset(eventhubsParams: Predef.Map[String, String],
                                   offsetStore: OffsetStore): (EventHubsOffsetType, String) = {
    // Determine the offset to start receiving data
    val previousOffset = offsetStore.read()
    EventHubsClientWrapper.configureStartOffset(previousOffset, eventhubsParams)
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

  /**
   * create a client without initializing receivers
   *
   * the major purpose of this API is for creating AMQP management client
   */
  def createClient(eventhubsParams: Map[String, String]): EventHubClient =
    EventHubClient.createFromConnectionStringSync(connectionString.toString)

  def createReceiver(partitionId: String,
                     startOffset: String,
                     offsetType: EventHubsOffsetType,
                     maximumEventRate: Int): Unit = {
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    createReceiverInternal(partitionId, offsetType, startOffset)
  }

  def createReceiver(ehParams: Map[String, String],
                     partitionId: String,
                     offsetStore: OffsetStore,
                     maximumEventRate: Int): Unit = {
    val (offsetType, currentOffset) =
      configureStartOffset(ehParams, offsetStore)
    logInfo(s"start a receiver for partition $partitionId with the start offset $currentOffset")
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    createReceiverInternal(partitionId, offsetType, currentOffset)
  }

  private[spark] def createReceiverInternal(partitionId: String,
                                            offsetType: EventHubsOffsetType,
                                            currentOffset: String): Unit = {
    eventhubsClient = EventHubClient.createFromConnectionStringSync(connectionString)

    eventhubsReceiver = offsetType match {
      case EventHubsOffsetTypes.None | EventHubsOffsetTypes.PreviousCheckpoint |
          EventHubsOffsetTypes.InputByteOffset =>
        if (receiverEpoch > DEFAULT_RECEIVER_EPOCH) {
          eventhubsClient.createEpochReceiverSync(consumerGroup,
                                                  partitionId,
                                                  currentOffset,
                                                  receiverEpoch)
        } else {
          eventhubsClient.createReceiverSync(consumerGroup, partitionId, currentOffset)
        }
      case EventHubsOffsetTypes.InputTimeOffset =>
        if (receiverEpoch > DEFAULT_RECEIVER_EPOCH) {
          eventhubsClient.createEpochReceiverSync(consumerGroup,
                                                  partitionId,
                                                  Instant.ofEpochSecond(currentOffset.toLong),
                                                  receiverEpoch)
        } else {
          eventhubsClient.createReceiverSync(consumerGroup,
                                             partitionId,
                                             Instant.ofEpochSecond(currentOffset.toLong))
        }
    }

    eventhubsReceiver.setPrefetchCount(MAXIMUM_PREFETCH_COUNT)
  }

  /**
   * starting from EventHubs client 0.13.1, returning a null from receiver means that there is
   * no message in server end
   */
  def receive(expectedEventNum: Int): Iterable[EventData] = {
    val events = eventhubsReceiver
      .receive(math.min(expectedEventNum, eventhubsReceiver.getPrefetchCount))
      .get()
    if (events != null) events.asScala else null
  }

  override def close(): Unit = {
    if (eventhubsReceiver != null) eventhubsReceiver.closeSync()
    if (eventhubsClient != null) eventhubsClient.closeSync()
  }

  def closeReceiver(): Unit = {
    eventhubsReceiver.closeSync()
  }

  override def endPointOfPartition(retryIfFail: Boolean,
                                   targetEventHubsNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    throw new UnsupportedOperationException(
      "endPointOfPartition is not supported by this client" +
        " yet, please use AMQPEventHubsClient")
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Predef.Map[EventHubNameAndPartition, Long]] = {
    throw new UnsupportedOperationException(
      "lastEnqueueTimeOfPartitions is not supported by this" +
        " client yet, please use AMQPEventHubsClient")
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def startSeqOfPartition(retryIfFail: Boolean,
                                   targetEventHubNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Predef.Map[EventHubNameAndPartition, Long]] = {
    throw new UnsupportedOperationException(
      "startSeqOfPartition is not supported by this client" +
        " yet, please use AMQPEventHubsClient")
  }
}

private[spark] object EventHubsClientWrapper {
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

  def getEventHubReceiver(ehParams: Map[String, String],
                          partitionId: Int,
                          startOffset: Long,
                          offsetType: EventHubsOffsetType,
                          maximumEventRate: Int): EventHubsClientWrapper = {
    val ehName = ehParams.get("eventhubs.name").toString
    val eventHubClientWrapperInstance = new EventHubsClientWrapper(Map(ehName -> ehParams))
    eventHubClientWrapperInstance.createReceiver(partitionId.toString,
                                                 startOffset.toString,
                                                 offsetType,
                                                 maximumEventRate)
    eventHubClientWrapperInstance
  }
}
