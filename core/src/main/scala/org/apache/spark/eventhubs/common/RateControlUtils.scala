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

  private[spark] def calculateStartOffset(
      nameAndPartition: NameAndPartition,
      filteringOffsetAndType: Map[NameAndPartition, (EventHubsOffsetType, Long)],
      startOffsetInNextBatch: Map[NameAndPartition, (Long, Long)]): (EventHubsOffsetType, Long) = {
    filteringOffsetAndType.getOrElse(
      nameAndPartition,
      (EventHubsOffsetTypes.PreviousCheckpoint, startOffsetInNextBatch(nameAndPartition)._1)
    )
  }

  private[spark] def validateFilteringParams(eventHubsClients: Map[String, Client],
                                             ehParams: Map[String, _],
                                             namesAndPartitions: List[NameAndPartition]): Unit = {
    val lastEnqueuedTimes: List[(NameAndPartition, Long)] = for {
      nAndP <- namesAndPartitions
      name = nAndP.ehName
      lastTime = eventHubsClients(name).lastEnqueuedTime(nAndP).get
    } yield nAndP -> lastTime
    val booleans: List[Boolean] = for {
      (nAndP, lastTime) <- lastEnqueuedTimes
      passInEnqueueTime = ehParams.get(nAndP.ehName) match {
        case Some(x) =>
          x.asInstanceOf[Map[String, String]]
            .getOrElse("eventhubs.filter.enqueuetime", EventHubsUtils.DefaultEnqueueTime)
        case None =>
          ehParams
            .asInstanceOf[Map[String, String]]
            .getOrElse("eventhubs.filter.enqueuetime", EventHubsUtils.DefaultEnqueueTime)
      }
    } yield lastTime >= passInEnqueueTime.toLong
    require(!booleans.contains(false),
            "You cannot pass in an enqueue time that is greater than what exists in EventHubs.")
  }

  private[spark] def composeFromOffsetWithFilteringParams(
      ehParams: Map[String, _],
      startOffsetsAndSeqNos: Map[NameAndPartition, (Long, Long)])
    : Map[NameAndPartition, (EventHubsOffsetType, Long)] = {
    for {
      (nameAndPartition, (offset, _)) <- startOffsetsAndSeqNos
      (offsetType, offsetStr) = configureStartOffset(
        offset.toString,
        ehParams.get(nameAndPartition.ehName) match {
          case Some(x) => x.asInstanceOf[Map[String, String]]
          case None    => ehParams.asInstanceOf[Map[String, String]]
        }
      )
    } yield (nameAndPartition, (offsetType, offsetStr.toLong))
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
