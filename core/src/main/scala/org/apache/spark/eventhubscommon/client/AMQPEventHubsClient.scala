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
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import com.microsoft.azure.eventhubs.{EventHubClient => AzureEventHubClient, EventHubPartitionRuntimeInformation}
import com.microsoft.azure.servicebus.amqp.AmqpException

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.internal.Logging

private[client] class AMQPEventHubsClient(
    eventHubNamespace: String,
    eventHubsNames: List[String],
    ehParams: Map[String, Map[String, String]]) extends EventHubClient with Logging {

  private val ehNameToClient = new mutable.HashMap[String, AzureEventHubClient]
  private val maxRetry = 3

  init()

  private def init(): Unit = {
    createLinkToEventHubs(eventHubsNames)
  }

  private def createLinkToEventHubs(ehNames: List[String]): Unit = {
    for (ehName <- ehNames) {
      ehNameToClient += ehName -> new EventHubsClientWrapper().createClient(ehParams(ehName))
    }
  }

  private def getRuntTimeInfoOfPartitionsInner(
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]) = {
    val results = new mutable.HashMap[EventHubNameAndPartition,
      EventHubPartitionRuntimeInformation]()
    val failedEventHubsPartitionList = new ListBuffer[EventHubNameAndPartition]
    for (ehNameAndPartition <- targetEventHubNameAndPartitions) {
      try {
        val ehName = ehNameAndPartition.eventHubName
        val partitionId = ehNameAndPartition.partitionId
        val client = ehNameToClient.get(ehName)
        require(client.isDefined, "cannot find client for EventHubs instance " + ehName)
        val runTimeInfo = client.get.getPartitionRuntimeInformation(partitionId.toString).get()
        results += ehNameAndPartition -> runTimeInfo
      } catch {
        case e: AmqpException =>
          logInfo(s"the connection to ${ehNameAndPartition.eventHubName}" +
            s" might has been closed by EventHubs, replacing with new link", e)
          failedEventHubsPartitionList += ehNameAndPartition
        case NonFatal(e) =>
          logError(s"Cannot get runtime info for $ehNameAndPartition", e)
          failedEventHubsPartitionList += ehNameAndPartition
        case e =>
          logError("unrecoverable error", e)
          throw e
      }
    }
    (results, failedEventHubsPartitionList)
  }

  private def getRunTimeInfoOfPartitions(
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Map[EventHubNameAndPartition, EventHubPartitionRuntimeInformation] = {
    val results = new mutable.HashMap[EventHubNameAndPartition, EventHubPartitionRuntimeInformation]
    var ehPartitionToTryThisRound = targetEventHubNameAndPartitions
    for (currentRound <- 0 until maxRetry) {
      val (resultsInThisRound, ehPartitionToTryNextRound) = getRuntTimeInfoOfPartitionsInner(
        ehPartitionToTryThisRound)
      results ++= resultsInThisRound
      if (ehPartitionToTryNextRound.nonEmpty) {
        createLinkToEventHubs(ehPartitionToTryNextRound.map(_.eventHubName).distinct.toList)
        ehPartitionToTryThisRound = ehPartitionToTryNextRound.toList
        ehPartitionToTryNextRound.clear()
        logWarning(s"failed to fetch runtime info for $ehPartitionToTryThisRound, has tried" +
          s" for ${currentRound + 1} times")
      } else {
        return results.toMap
      }
    }
    if (results.size < targetEventHubNameAndPartitions.size) {
      throw new IllegalStateException(s"cannot fetch runtime info for" +
        s" $ehPartitionToTryThisRound from EventHubs after retrying for $maxRetry times," +
        s" please check the availability of EventHubs")
    }
    results.toMap
  }

  /**
   * return the end point of each partition
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    val runtimeInformation = getRunTimeInfoOfPartitions(targetEventHubNameAndPartitions)
    Some(runtimeInformation.map { case (ehNameAndPartition, runTimeInfo) =>
      (ehNameAndPartition, (runTimeInfo.getLastEnqueuedOffset.toLong,
        runTimeInfo.getLastEnqueuedSequenceNumber))})
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, Long]] = {
    val runtimeInformation = getRunTimeInfoOfPartitions(targetEventHubNameAndPartitions)
    Some(runtimeInformation.map { case (ehNameAndPartition, runTimeInfo) =>
      (ehNameAndPartition, runTimeInfo.getLastEnqueuedTimeUtc.getEpochSecond)})
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def startSeqOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, Long]] = {
    val runtimeInformation = getRunTimeInfoOfPartitions(targetEventHubNameAndPartitions)
    Some(runtimeInformation.map { case (ehNameAndPartition, runTimeInfo) =>
      (ehNameAndPartition, runTimeInfo.getBeginSequenceNumber)})
  }

  /**
   * close this client
   */
  override def close(): Unit = {
    for ((_, ehClient) <- ehNameToClient) {
      ehClient.closeSync()
    }
  }


}

private[spark] object AMQPEventHubsClient {

  def getInstance(eventHubsNamespace: String, eventhubsParams: Map[String, Map[String, String]]):
    AMQPEventHubsClient = {
    new AMQPEventHubsClient(eventHubsNamespace, eventhubsParams.keys.toList, eventhubsParams)
  }
}
