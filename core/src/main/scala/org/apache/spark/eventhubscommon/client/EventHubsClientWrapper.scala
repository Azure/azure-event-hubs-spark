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

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClientWrapper(private val ehParams: Map[String, String])
    extends Serializable
    with Client
    with Logging {

  override private[spark] var client: EventHubClient = _
  private[spark] var partitionReceiver: PartitionReceiver = _

  /* Extract relevant info from ehParams */
  private val ehNamespace = ehParams("eventhubs.namespace").toString
  private val ehName = ehParams("eventhubs.name").toString
  private val ehPolicyName = ehParams("eventhubs.policyname").toString
  private val ehPolicy = ehParams("eventhubs.policykey").toString
  private val consumerGroup = ehParams
    .getOrElse("eventhubs.consumergroup", EventHubClient.DEFAULT_CONSUMER_GROUP_NAME)
    .toString
  private val connectionString =
    new ConnectionStringBuilder(ehNamespace, ehName, ehPolicyName, ehPolicy).toString

  private[spark] def initClient() =
    client = EventHubClient.createFromConnectionStringSync(connectionString)

  private[spark] def initReceiver(partitionId: String,
                                  offsetType: EventHubsOffsetType,
                                  currentOffset: String): Unit = {
    logInfo(
      s"createReceiverInternal: Starting a receiver for partitionId $partitionId with start offset $currentOffset")
    client = EventHubClient.createFromConnectionStringSync(connectionString)
    partitionReceiver = offsetType match {
      case EventHubsOffsetTypes.EnqueueTime =>
        client.createReceiverSync(consumerGroup,
                                  partitionId,
                                  Instant.ofEpochSecond(currentOffset.toLong))
      case _ =>
        client.createReceiverSync(consumerGroup, partitionId, currentOffset)
    }
  }

  /**
   * starting from EventHubs client 0.13.1, returning a null from receiver means that there is
   * no message in server end
   */
  def receive(expectedEventNum: Int): Iterable[EventData] = {
    // TODO: revisit this method after refactoring the RDD. We should not need to call min like this.
    val events = partitionReceiver
      .receive(math.min(expectedEventNum, partitionReceiver.getPrefetchCount))
      .get()
    if (events != null) events.asScala else null
  }

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfoOfPartitions(ehNameAndPartition: EventHubNameAndPartition) = {
    try {
      val partitionId = ehNameAndPartition.partitionId
      client.getPartitionRuntimeInformation(partitionId.toString).get
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def startSeqOfPartition(
      eventHubNameAndPartition: EventHubNameAndPartition): Option[Long] = {
    try {
      val runtimeInformation = getRunTimeInfoOfPartitions(eventHubNameAndPartition)
      Some(runtimeInformation.getBeginSequenceNumber)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  /**
   * return the end point of each partition
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  override def endPointOfPartition(
      eventHubNameAndPartition: EventHubNameAndPartition): Option[(Long, Long)] = {
    try {
      val runtimeInfo = getRunTimeInfoOfPartitions(eventHubNameAndPartition)
      Some((runtimeInfo.getLastEnqueuedOffset.toLong, runtimeInfo.getLastEnqueuedSequenceNumber))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      eventHubNameAndPartition: EventHubNameAndPartition): Option[Long] = {
    try {
      val runtimeInfo = getRunTimeInfoOfPartitions(eventHubNameAndPartition)
      Some(runtimeInfo.getLastEnqueuedTimeUtc.getEpochSecond)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  override def close(): Unit = {
    logInfo("close: Closing EventHubsClientWrapper.")
    if (partitionReceiver != null) partitionReceiver.closeSync()
    if (client != null) client.closeSync()
  }

}

private[spark] object EventHubsClientWrapper {
  private[eventhubscommon] def configureStartOffset(
      previousOffset: String,
      eventhubsParams: Map[String, String]): (EventHubsOffsetType, String) = {
    if (previousOffset != "-1" && previousOffset != null) {
      (EventHubsOffsetTypes.PreviousCheckpoint, previousOffset)
    } else if (eventhubsParams.contains("eventhubs.filter.offset")) {
      (EventHubsOffsetTypes.InputByteOffset, eventhubsParams("eventhubs.filter.offset"))
    } else if (eventhubsParams.contains("eventhubs.filter.enqueuetime")) {
      (EventHubsOffsetTypes.EnqueueTime, eventhubsParams("eventhubs.filter.enqueuetime"))
    } else {
      (EventHubsOffsetTypes.None, PartitionReceiver.START_OF_STREAM)
    }
  }

  private[spark] def apply(ehParams: Map[String, String]): EventHubsClientWrapper =
    new EventHubsClientWrapper(ehParams)

  // TODO: This will be re-introduced in the next phase of re-write
  /*
  private[spark] def apply(ehParams: Map[String, String],
                           partitionId: String,
                           offsetType: EventHubsOffsetType,
                           currentOffset: String): EventHubsClientWrapper = {
    val client = new EventHubsClientWrapper(ehParams)
    client.createReceiver(partitionId, offsetType, currentOffset)
    client
  }
 */
}
