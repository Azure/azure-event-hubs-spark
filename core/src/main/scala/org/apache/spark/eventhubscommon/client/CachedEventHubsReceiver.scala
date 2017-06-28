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

import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.microsoft.azure.eventhubs.{EventHubClient => AzureEventHubClient, PartitionReceiver}

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.Common.{configureGeneralParameters, configureMaxEventRate, createNewReceiver, MAXIMUM_EVENT_RATE}
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.internal.Logging

private[spark] class CachedEventHubsReceiver(batchInterval: Int) extends Logging {

  def getOrCreateReceiver(
      eventhubsParams: Predef.Map[String, String],
      partitionId: String,
      startOffset: String,
      offsetType: EventHubsOffsetType,
      maximumEventRate: Int,
      batchInternal: Int,
      currentTimestamp: Int): PartitionReceiver = {
    import CachedEventHubsReceiver._
    val (connectionString, consumerGroup, receiverEpoch) = configureGeneralParameters(
      eventhubsParams)
    val currentOffset = startOffset
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    if (eventhubsClient == null) {
      eventhubsClient = AzureEventHubClient.createFromConnectionStringSync(
        connectionString.toString)
      receiverCache = initReceiverCache(batchInternal)
    }
    val ehName = eventhubsParams("eventhubs.name")
    val ehNameAndPartition = EventHubNameAndPartition(ehName, partitionId.toInt)
    val (cachedReceiver, ts) = receiverCache.getIfPresent(ehNameAndPartition)
    if (cachedReceiver != null && ts == currentTimestamp - batchInternal) {
      receiverCache.put(ehNameAndPartition, (cachedReceiver, currentTimestamp))
      cachedReceiver
    } else {
      logInfo(s"missed receiverCache for $ehNameAndPartition at $ts")
      val newReceiver = createNewReceiver(eventhubsClient,
        consumerGroup, partitionId, offsetType, currentOffset, receiverEpoch)
      receiverCache.put(ehNameAndPartition, (newReceiver, ts))
      newReceiver
    }
  }
}

private object CachedEventHubsReceiver {

  private var eventhubsClient: AzureEventHubClient = _
  private var receiverCache: Cache[EventHubNameAndPartition, (PartitionReceiver, Int)] = _

  private def initReceiverCache(batchInterval: Int):
    Cache[EventHubNameAndPartition, (PartitionReceiver, Int)] = {
    CacheBuilder.newBuilder()
      .expireAfterWrite(batchInterval * 4, TimeUnit.MILLISECONDS)
      .removalListener(new RemovalListener[EventHubNameAndPartition, (PartitionReceiver, Int)]() {
        override def onRemoval(removal: RemovalNotification[EventHubNameAndPartition,
          (PartitionReceiver, Int)]) {
          val (receiver, _) = removal.getValue
          receiver.closeSync()
        }
      })
      .build[EventHubNameAndPartition, (PartitionReceiver, Int)]()
  }
}
