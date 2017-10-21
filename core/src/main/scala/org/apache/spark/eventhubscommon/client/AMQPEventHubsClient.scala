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

import scala.collection.mutable
import com.microsoft.azure.eventhubs.{
  EventData,
  EventHubClient,
  EventHubPartitionRuntimeInformation
}
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.internal.Logging

private[client] class AMQPEventHubsClient(private val ehParams: Map[String, String])
    extends Serializable
    with Client
    with Logging {

  // TODO: these methods will be gone after client re-write is done.
  override private[spark] var client: EventHubClient = _
  override def initClient(): Unit = {}

  override def initReceiver(partitionId: String,
                            offsetType: EventHubsOffsetType,
                            currentOffset: String): Unit = {}

  override def receive(expectedEvents: Int): Iterable[EventData] = {
    Iterable[EventData]()
  }

  // --------------------------

  private val nameToClient: EventHubsClientWrapper = EventHubsClientWrapper(ehParams)
  nameToClient.initClient()

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfoOfPartitions(ehNameAndPartition: EventHubNameAndPartition) = {
    try {
      val partitionId = ehNameAndPartition.partitionId
      val client = nameToClient.asInstanceOf[EventHubsClientWrapper].client
      client.getPartitionRuntimeInformation(partitionId.toString).get
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  // TODO: this will ultimately be deleted
  private def getRunTimeInfoOfPartitions(
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]) = {
    val results = new mutable.HashMap[EventHubNameAndPartition, EventHubPartitionRuntimeInformation]
    try {
      for (ehNameAndPartition <- targetEventHubNameAndPartitions) {
        val partitionId = ehNameAndPartition.partitionId
        val client = nameToClient.asInstanceOf[EventHubsClientWrapper].client
        val runTimeInfo =
          client.getPartitionRuntimeInformation(partitionId.toString).get()
        results += ehNameAndPartition -> runTimeInfo
      }
      results.toMap.view
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
   * close this client
   */
  override def close(): Unit = {
    logInfo("close: Closing AMQPEventHubClient.")
    nameToClient.close()
  }
}

private[spark] object AMQPEventHubsClient {
  def apply(eventhubsParams: Map[String, String]): AMQPEventHubsClient =
    new AMQPEventHubsClient(eventhubsParams)
}
