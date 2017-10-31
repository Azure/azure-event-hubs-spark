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

private[spark] object RateControlUtils extends Logging {

  /**
   * return the last sequence number of each partition, which are to be
   * received in this micro batch
   *
   * @param highestEndpoints the latest offset/seq of each partition
   */
  private[spark] def clamp(currentOffsetsAndSeqNos: Map[NameAndPartition, (Long, Long)],
                           highestEndpoints: List[(NameAndPartition, (Long, Long))],
                           ehConf: EventHubsConf): Map[NameAndPartition, Long] = {
    (for {
      (nAndP, (_, seqNo)) <- highestEndpoints
      maxRate: Long = ehConf.maxRatePerPartition(nAndP.partitionId)
      endSeqNo = math.min(seqNo, maxRate + currentOffsetsAndSeqNos(nAndP)._2)
    } yield (nAndP, endSeqNo)) toMap
  }

  private[spark] def calculateStartOffset(
      nameAndPartition: NameAndPartition,
      filteringOffsetAndType: Map[NameAndPartition, (EventHubsOffsetType, Offset)],
      startOffsetInNextBatch: Map[NameAndPartition, (Long, Long)]): (EventHubsOffsetType, Long) = {
    filteringOffsetAndType.getOrElse(
      nameAndPartition,
      (EventHubsOffsetTypes.PreviousCheckpoint, startOffsetInNextBatch(nameAndPartition)._1)
    )
  }

  private[spark] def validateFilteringParams(ehClient: Client,
                                             ehConf: EventHubsConf,
                                             namesAndPartitions: List[NameAndPartition]): Unit = {
    val lastEnqueuedTimes: List[(NameAndPartition, Long)] = for {
      nAndP <- namesAndPartitions
      lastTime = ehClient.lastEnqueuedTime(nAndP).get
    } yield nAndP -> lastTime
    val booleans: List[Boolean] = for {
      (nAndP, lastTime) <- lastEnqueuedTimes
      passInEnqueueTime = ehConf.startEnqueueTimes(nAndP.partitionId)
    } yield lastTime >= passInEnqueueTime.toLong
    require(!booleans.contains(false),
            "You cannot pass in an enqueue time that is greater than what exists in EventHubs.")
  }

  private[spark] def composeFromOffsetWithFilteringParams(
      ehConf: EventHubsConf,
      startOffsetsAndSeqNos: Map[NameAndPartition, (Offset, SequenceNumber)])
    : Map[NameAndPartition, (EventHubsOffsetType, Offset)] = {
    for {
      (nAndP, (offset, _)) <- startOffsetsAndSeqNos
      (offsetType, startOffset) = configureStartOffset(nAndP.partitionId, offset.toString, ehConf)
    } yield (nAndP, (offsetType, startOffset))
  }

  private[eventhubs] def configureStartOffset(
      partitionId: PartitionId,
      previousOffset: String,
      ehConf: EventHubsConf): (EventHubsOffsetType, Offset) = {
    if (previousOffset != "-1" && previousOffset != null) {
      (EventHubsOffsetTypes.PreviousCheckpoint, previousOffset.toLong)
    } else if (ehConf.startOffsets != null) {
      (EventHubsOffsetTypes.Offset, ehConf.startOffsets(partitionId))
    } else if (ehConf.startEnqueueTimes != null) {
      (EventHubsOffsetTypes.EnqueueTime, ehConf.startEnqueueTimes(partitionId))
    } else {
      (EventHubsOffsetTypes.None, EventHubsUtils.StartOfStream.toLong)
    }
  }
}
