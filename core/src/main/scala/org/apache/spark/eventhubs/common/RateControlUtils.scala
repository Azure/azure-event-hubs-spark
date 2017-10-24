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

package org.apache.spark.eventhubs.common

import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubs.common.client.{
  Client,
  EventHubsClientWrapper,
  EventHubsOffsetTypes
}
import org.apache.spark.internal.Logging

import scala.collection.mutable

private[spark] object RateControlUtils extends Logging {

  private def maxRateLimitPerPartition(ehName: String, ehParams: Map[String, _]): Int = {
    val maxRate = ehParams.get(ehName) match {
      case Some(params) =>
        // Must be DirectDStream
        params
          .asInstanceOf[Map[String, String]]
          .getOrElse("eventhubs.maxRate", "10000")
          .toInt
      case None =>
        // Must be Structured Stream
        ehParams
          .asInstanceOf[Map[String, String]]
          .getOrElse("eventhubs.maxRate", "10000")
          .toInt
    }
    require(maxRate > 0,
            s"eventhubs.maxRate has to be larger than zero, violated by $ehName ($maxRate)")
    maxRate
  }

  /**
   * return the last sequence number of each partition, which are to be
   * received in this micro batch
   *
   * @param highestEndpoints the latest offset/seq of each partition
   */
  private def defaultRateControl(currentOffsetsAndSeqNums: Map[NameAndPartition, (Long, Long)],
                                 highestEndpoints: Map[NameAndPartition, (Long, Long)],
                                 ehParams: Map[String, _]): Map[NameAndPartition, Long] = {
    highestEndpoints.map {
      case (eventHubNameAndPar, (_, latestSeq)) =>
        val maximumAllowedMessageCnt =
          maxRateLimitPerPartition(eventHubNameAndPar.ehName, ehParams)
        val endSeq =
          math.min(latestSeq,
                   maximumAllowedMessageCnt + currentOffsetsAndSeqNums(eventHubNameAndPar)._2)
        (eventHubNameAndPar, endSeq)
    }
  }

  private[spark] def clamp(currentOffsetsAndSeqNums: Map[NameAndPartition, (Long, Long)],
                           highestEndpoints: Map[NameAndPartition, (Long, Long)],
                           ehParams: Map[String, _]): Map[NameAndPartition, Long] = {
    defaultRateControl(currentOffsetsAndSeqNums, highestEndpoints, ehParams)
  }

  private[spark] def fetchLatestOffset(
      eventHubClients: Map[String, Client],
      fetchedHighestOffsetsAndSeqNums: Map[NameAndPartition, (Long, Long)])
    : Option[Map[NameAndPartition, (Long, Long)]] = {

    val r = new mutable.HashMap[NameAndPartition, (Long, Long)].empty
    for (nameAndPartition <- fetchedHighestOffsetsAndSeqNums.keySet) {
      val name = nameAndPartition.ehName
      val endPoint = eventHubClients(name).lastSeqAndOffset(nameAndPartition)
      require(endPoint.isDefined, s"Failed to get ending sequence number for $nameAndPartition")

      r += nameAndPartition -> endPoint.get
    }

    val mergedOffsets = if (fetchedHighestOffsetsAndSeqNums != null) {
      fetchedHighestOffsetsAndSeqNums ++ r
    } else {
      r.toMap
    }
    Option(mergedOffsets)
  }

  private[spark] def validateFilteringParams(eventHubsClients: Map[String, Client],
                                             ehParams: Map[String, _],
                                             ehNameAndPartitions: List[NameAndPartition]): Unit = {

    // first check if the parameters are valid
    val latestEnqueueTimeOfPartitions = new mutable.HashMap[NameAndPartition, Long].empty
    for (nameAndPartition <- ehNameAndPartitions) {
      val name = nameAndPartition.ehName
      val lastEnqueueTime = eventHubsClients(name).lastEnqueuedTime(nameAndPartition).get

      latestEnqueueTimeOfPartitions += nameAndPartition -> lastEnqueueTime
    }

    latestEnqueueTimeOfPartitions.toMap.foreach {
      case (ehNameAndPartition, latestEnqueueTime) =>
        val passInEnqueueTime = ehParams.get(ehNameAndPartition.ehName) match {
          case Some(ehParams) =>
            ehParams
              .asInstanceOf[Map[String, String]]
              .getOrElse("eventhubs.filter.enqueuetime", Long.MinValue.toString)
              .toLong
          case None =>
            ehParams
              .asInstanceOf[Map[String, String]]
              .getOrElse("eventhubs.filter.enqueuetime", Long.MinValue.toString)
              .toLong
        }
        require(
          latestEnqueueTime >= passInEnqueueTime,
          "you cannot pass in an enqueue time which is later than the highest enqueue time in" +
            s" event hubs, ($ehNameAndPartition, pass-in-enqueuetime $passInEnqueueTime," +
            s" latest-enqueuetime $latestEnqueueTime)"
        )
    }
  }

  private[spark] def composeFromOffsetWithFilteringParams(
      ehParams: Map[String, _],
      fetchedStartOffsetsInNextBatch: Map[NameAndPartition, (Long, Long)])
    : Map[NameAndPartition, (EventHubsOffsetType, Long)] = {

    fetchedStartOffsetsInNextBatch.map {
      case (ehNameAndPartition, (offset, _)) =>
        val (offsetType, offsetStr) = EventHubsClientWrapper.configureStartOffset(
          offset.toString,
          ehParams.get(ehNameAndPartition.ehName) match {
            case Some(ehConfig) =>
              ehConfig.asInstanceOf[Map[String, String]]
            case None =>
              ehParams.asInstanceOf[Map[String, String]]
          }
        )
        (ehNameAndPartition, (offsetType, offsetStr.toLong))
    }
  }

  private[spark] def calculateStartOffset(
      ehNameAndPartition: NameAndPartition,
      filteringOffsetAndType: Map[NameAndPartition, (EventHubsOffsetType, Long)],
      startOffsetInNextBatch: Map[NameAndPartition, (Long, Long)]): (EventHubsOffsetType, Long) = {
    filteringOffsetAndType.getOrElse(
      ehNameAndPartition,
      (EventHubsOffsetTypes.PreviousCheckpoint, startOffsetInNextBatch(ehNameAndPartition)._1)
    )
  }
}
