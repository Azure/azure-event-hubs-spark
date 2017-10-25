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
import org.apache.spark.eventhubs.common.client.{ Client, EventHubsOffsetTypes }
import org.apache.spark.internal.Logging

import scala.collection.mutable

private[spark] object RateControlUtils extends Logging {

  /**
   * return the last sequence number of each partition, which are to be
   * received in this micro batch
   *
   * @param highestEndpoints the latest offset/seq of each partition
   */
  // TODO: a lot of this code comes from the fact that EHSource takes Map[String, String] and
  // TODO: DStream takes Map[String, Map[String, String]]. Which is preferred? We should pick one.
  private[spark] def clamp(currentOffsetsAndSeqNos: Map[NameAndPartition, (Long, Long)],
                           highestEndpoints: List[(NameAndPartition, (Long, Long))],
                           ehParams: Map[String, _]): Map[NameAndPartition, Long] = {
    (for {
      (nameAndPartition, (_, seqNo)) <- highestEndpoints
      maxRate = ehParams.get(nameAndPartition.ehName) match {
        case Some(x) => // for DStream
          x.asInstanceOf[Map[String, String]]
            .getOrElse("eventhubs.maxRate", EventHubsUtils.DefaultMaxRate)
        case None => // for Structured Stream
          ehParams
            .asInstanceOf[Map[String, String]]
            .getOrElse("eventhubs.maxRate", EventHubsUtils.DefaultMaxRate)
      }
      endSeqNo = math.min(seqNo, maxRate.toInt + currentOffsetsAndSeqNos(nameAndPartition)._2)
    } yield (nameAndPartition, endSeqNo)) toMap
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
          case Some(params) =>
            params
              .asInstanceOf[Map[String, String]]
              .getOrElse("eventhubs.filter.enqueuetime", EventHubsUtils.DefaultEnqueueTime)
              .toLong
          case None =>
            ehParams
              .asInstanceOf[Map[String, String]]
              .getOrElse("eventhubs.filter.enqueuetime", EventHubsUtils.DefaultEnqueueTime)
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
        val (offsetType, offsetStr) = configureStartOffset(
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

  private[eventhubs] def configureStartOffset(
      previousOffset: String,
      ehParams: Map[String, String]): (EventHubsOffsetType, String) = {
    if (previousOffset != "-1" && previousOffset != null) {
      (EventHubsOffsetTypes.PreviousCheckpoint, previousOffset)
    } else if (ehParams.contains("eventhubs.filter.offset")) {
      (EventHubsOffsetTypes.Offset, ehParams("eventhubs.filter.offset"))
    } else if (ehParams.contains("eventhubs.filter.enqueuetime")) {
      (EventHubsOffsetTypes.EnqueueTime, ehParams("eventhubs.filter.enqueuetime"))
    } else {
      (EventHubsOffsetTypes.None, EventHubsUtils.StartOfStream)
    }
  }
}
