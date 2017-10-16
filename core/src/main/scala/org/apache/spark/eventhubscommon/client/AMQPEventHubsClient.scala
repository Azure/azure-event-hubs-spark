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
import com.microsoft.azure.eventhubs.{ EventHubClient, EventHubPartitionRuntimeInformation }
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.internal.Logging

private[client] class AMQPEventHubsClient(
    ehNames: List[String],
    ehParams: Map[String, Map[String, String]])
    extends Client
    with Logging {

  private val nameToClient = new mutable.HashMap[String, EventHubClient]
  for (ehName <- ehNames)
    nameToClient += ehName -> new EventHubsClientWrapper(ehParams)
      .createClient(ehParams(ehName))

  private def getRunTimeInfoOfPartitions(
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]) = {
    val results = new mutable.HashMap[EventHubNameAndPartition,
                                      EventHubPartitionRuntimeInformation]
    try {
      for (ehNameAndPartition <- targetEventHubNameAndPartitions) {
        val ehName = ehNameAndPartition.eventHubName
        val partitionId = ehNameAndPartition.partitionId
        val client = nameToClient.get(ehName)
        require(client.isDefined,
                "cannot find client for EventHubs instance " + ehName)
        val runTimeInfo =
          client.get.getPartitionRuntimeInformation(partitionId.toString).get()
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
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    try {
      val runtimeInformation = getRunTimeInfoOfPartitions(
        targetEventHubNameAndPartitions)
      Some(runtimeInformation.map {
        case (ehNameAndPartition, runTimeInfo) =>
          (ehNameAndPartition,
           (runTimeInfo.getLastEnqueuedOffset.toLong,
            runTimeInfo.getLastEnqueuedSequenceNumber))
      }.toMap)
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
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Map[EventHubNameAndPartition, Long]] = {
    try {
      val runtimeInformation = getRunTimeInfoOfPartitions(
        targetEventHubNameAndPartitions)
      Some(runtimeInformation.map {
        case (ehNameAndPartition, runTimeInfo) =>
          (ehNameAndPartition,
           runTimeInfo.getLastEnqueuedTimeUtc.getEpochSecond)
      }.toMap)
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
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Map[EventHubNameAndPartition, Long]] = {
    try {
      val runtimeInformation = getRunTimeInfoOfPartitions(
        targetEventHubNameAndPartitions)
      Some(runtimeInformation.map {
        case (ehNameAndPartition, runTimeInfo) =>
          (ehNameAndPartition, runTimeInfo.getBeginSequenceNumber)
      }.toMap)
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
    for ((_, ehClient) <- nameToClient) {
      ehClient.closeSync()
    }
  }
}

private[spark] object AMQPEventHubsClient {
  def getInstance(eventHubsNamespace: String,
                  eventhubsParams: Map[String, Map[String, String]])
    : AMQPEventHubsClient = {
    new AMQPEventHubsClient(eventhubsParams.keys.toList, eventhubsParams)
  }
}
