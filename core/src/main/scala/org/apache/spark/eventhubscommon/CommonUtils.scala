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

import org.apache.spark.eventhubscommon.client.EventHubClient
import org.apache.spark.internal.Logging

object CommonUtils extends Logging {

  private def maxRateLimitPerPartition(
      eventHubName: String,
      eventhubsParams: Map[String, _]): Int = {
    val maxRate = eventhubsParams.get(eventHubName) match {
      case Some(eventHubsConfigEntries) =>
        eventHubsConfigEntries.asInstanceOf[Map[String, String]].
          getOrElse("eventhubs.maxRate", "10000").toInt
      case None =>
        eventhubsParams.asInstanceOf[Map[String, String]].
          getOrElse("eventhubs.maxRate", "10000").toInt
    }
    require(maxRate > 0,
      s"eventhubs.maxRate has to be larger than zero, violated by $eventHubName ($maxRate)")
    maxRate
  }

  /**
   * return the last sequence number of each partition, which are to be received in this micro batch
   * @param highestEndpoints the latest offset/seq of each partition
   */
  private def defaultRateControl(
      currentOffsetsAndSeqNums: Map[EventHubNameAndPartition, (Long, Long)],
      highestEndpoints: Map[EventHubNameAndPartition, (Long, Long)],
      eventhubsParams: Map[String, _]): Map[EventHubNameAndPartition, Long] = {
    highestEndpoints.map{
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
      eventHubClient: EventHubClient, retryIfFail: Boolean):
      Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    eventHubClient.endPointOfPartition(retryIfFail)
  }
}
