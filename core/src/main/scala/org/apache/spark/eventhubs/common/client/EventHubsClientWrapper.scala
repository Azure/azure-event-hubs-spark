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

package org.apache.spark.eventhubs.common.client

import java.time.{Duration, Instant}

import scala.collection.JavaConverters._
import EventHubsOffsetTypes.EventHubsOffsetType
import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.common.NameAndPartition
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

  /* Establish connection */
  private val connectionString =
    new ConnectionStringBuilder(ehNamespace, ehName, ehPolicyName, ehPolicy).toString
  client = EventHubClient.createFromConnectionStringSync(connectionString)

  private[spark] def initReceiver(partitionId: String,
                                  offsetType: EventHubsOffsetType,
                                  currentOffset: String): Unit = {
    logInfo(
      s"createReceiverInternal: Starting a receiver for partitionId $partitionId with start offset $currentOffset")
    partitionReceiver = offsetType match {
      case EventHubsOffsetTypes.EnqueueTime =>
        client.createReceiverSync(consumerGroup,
                                  partitionId,
                                  Instant.ofEpochSecond(currentOffset.toLong))
      case _ =>
        client.createReceiverSync(consumerGroup, partitionId, currentOffset)
    }
    partitionReceiver.setReceiveTimeout(Duration.ofSeconds(5))
  }

  def receive(expectedEventNum: Int): Iterable[EventData] = {
    // TODO: revisit this method after refactoring the RDD. We should not need to call min like this.
    val events = partitionReceiver
      .receive(math.min(expectedEventNum, partitionReceiver.getPrefetchCount))
      .get()
    if (events != null) events.asScala else null
  }

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfo(nameAndPartition: NameAndPartition) = {
    try {
      val partitionId = nameAndPartition.partitionId.toString
      client.getPartitionRuntimeInformation(partitionId).get
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
  override def earliestSeqNo(nameAndPartition: NameAndPartition): Option[Long] = {
    try {
      val runtimeInformation = getRunTimeInfo(nameAndPartition)
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
  override def lastOffsetAndSeqNo(nameAndPartition: NameAndPartition): (Long, Long) = {
    try {
      val runtimeInfo = getRunTimeInfo(nameAndPartition)
      (runtimeInfo.getLastEnqueuedOffset.toLong, runtimeInfo.getLastEnqueuedSequenceNumber)
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
  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): Option[Long] = {
    try {
      val runtimeInfo = getRunTimeInfo(nameAndPartition)
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
  private[spark] def apply(ehParams: Map[String, String]): EventHubsClientWrapper =
    new EventHubsClientWrapper(ehParams)
}
