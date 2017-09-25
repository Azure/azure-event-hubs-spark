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

package org.apache.spark.eventhubscommon

import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper, EventHubsOffsetTypes}
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.internal.Logging

private[spark] object RateControlUtils extends Logging {

  private def maxRateLimitPerPartition(
      eventHubName: String,
      eventhubsParams: Map[String, _]): Int = {
    val maxRate = eventhubsParams.get(eventHubName) match {
      case Some(eventHubsConfigEntries) =>
        // this part shall be called by direct dstream where the parameters are indexed by eventhubs
        // names
        eventHubsConfigEntries.asInstanceOf[Map[String, String]].
          getOrElse("eventhubs.maxRate", "10000").toInt
      case None =>
        // this is called by structured streaming where eventhubsParams only contains the parameters
        // for a single eventhubs instance
        eventhubsParams.asInstanceOf[Map[String, String]].
          getOrElse("eventhubs.maxRate", "10000").toInt
    }
    require(maxRate > 0,
      s"eventhubs.maxRate has to be larger than zero, violated by $eventHubName ($maxRate)")
    maxRate
  }

  /**
   * return the last sequence number of each partition, which are to be
   * received in this micro batch
   *
   * @param highestEndpoints the latest offset/seq of each partition
   */
  private def defaultRateControl(
      currentOffsetsAndSeqNums: Map[EventHubNameAndPartition, (Long, Long)],
      highestEndpoints: Map[EventHubNameAndPartition, (Long, Long)],
      eventhubsParams: Map[String, _]): Map[EventHubNameAndPartition, Long] = {
    highestEndpoints.map {
      case (eventHubNameAndPar, (_, latestSeq)) =>
        val maximumAllowedMessageCnt = maxRateLimitPerPartition(
          eventHubNameAndPar.eventHubName, eventhubsParams)
        val endSeq = math.min(latestSeq,
          maximumAllowedMessageCnt + currentOffsetsAndSeqNums(eventHubNameAndPar)._2)
        (eventHubNameAndPar, endSeq)
    }
  }

  private[spark] def clamp(
      currentOffsetsAndSeqNums: Map[EventHubNameAndPartition, (Long, Long)],
      highestEndpoints: Map[EventHubNameAndPartition, (Long, Long)],
      eventhubsParams: Map[String, _]): Map[EventHubNameAndPartition, Long] = {
    defaultRateControl(currentOffsetsAndSeqNums, highestEndpoints, eventhubsParams)
  }

  private[spark] def fetchLatestOffset(
      eventHubClient: EventHubClient,
      retryIfFail: Boolean,
      fetchedHighestOffsetsAndSeqNums: Map[EventHubNameAndPartition, (Long, Long)]):
    Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    val r = eventHubClient.endPointOfPartition(retryIfFail, fetchedHighestOffsetsAndSeqNums.keySet.toList)
    if (r.isDefined) {
      // merge results
      val mergedOffsets = if (fetchedHighestOffsetsAndSeqNums != null) {
        fetchedHighestOffsetsAndSeqNums ++ r.get
      } else {
        r.get
      }
      Some(mergedOffsets)
    } else {
      r
    }
  }

  private[spark] def validateFilteringParams(
      eventHubsClient: EventHubClient,
      eventhubsParams: Map[String, _],
      ehNameAndPartitions: List[EventHubNameAndPartition]): Unit = {
    // first check if the parameters are valid
    val latestEnqueueTimeOfPartitions = eventHubsClient.lastEnqueueTimeOfPartitions(
      retryIfFail = true, ehNameAndPartitions)
    require(latestEnqueueTimeOfPartitions.isDefined, "cannot get latest enqueue time from Event" +
      " Hubs Rest Endpoint")
    latestEnqueueTimeOfPartitions.get.foreach {
      case (ehNameAndPartition, latestEnqueueTime) =>
        val passInEnqueueTime = eventhubsParams.get(ehNameAndPartition.eventHubName) match {
          case Some(ehParams) =>
            ehParams.asInstanceOf[Map[String, String]].getOrElse(
              "eventhubs.filter.enqueuetime", Long.MinValue.toString).toLong
          case None =>
            eventhubsParams.asInstanceOf[Map[String, String]].getOrElse(
              "eventhubs.filter.enqueuetime", Long.MinValue.toString).toLong
        }
        require(latestEnqueueTime >= passInEnqueueTime,
          "you cannot pass in an enqueue time which is later than the highest enqueue time in" +
            s" event hubs, ($ehNameAndPartition, pass-in-enqueuetime $passInEnqueueTime," +
            s" latest-enqueuetime $latestEnqueueTime)")
    }
  }

  private[spark] def composeFromOffsetWithFilteringParams(
      eventhubsParams: Map[String, _],
      fetchedStartOffsetsInNextBatch: Map[EventHubNameAndPartition, (Long, Long)]):
    Map[EventHubNameAndPartition, (EventHubsOffsetType, Long)] = {
    fetchedStartOffsetsInNextBatch.map {
      case (ehNameAndPartition, (offset, seq)) =>
        val (offsetType, offsetStr) = EventHubsClientWrapper.configureStartOffset(
          offset.toString,
          eventhubsParams.get(ehNameAndPartition.eventHubName) match {
            case Some(ehConfig) =>
              ehConfig.asInstanceOf[Map[String, String]]
            case None =>
              eventhubsParams.asInstanceOf[Map[String, String]]
          })
        (ehNameAndPartition, (offsetType, offsetStr.toLong))
    }
  }

  private[spark] def calculateStartOffset(
      ehNameAndPartition: EventHubNameAndPartition,
      filteringOffsetAndType: Map[EventHubNameAndPartition, (EventHubsOffsetType, Long)],
      startOffsetInNextBatch: Map[EventHubNameAndPartition, (Long, Long)]):
    (EventHubsOffsetType, Long) = {
    filteringOffsetAndType.getOrElse(
      ehNameAndPartition,
      (EventHubsOffsetTypes.PreviousCheckpoint, startOffsetInNextBatch(ehNameAndPartition)._1)
    )
  }
}
