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

import scala.collection.JavaConverters._

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.microsoft.azure.eventhubs.{EventData, EventHubClient => AzureEventHubClient, PartitionReceiver}

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.Common.{configureGeneralParameters, configureMaxEventRate, createNewReceiver, MAXIMUM_EVENT_RATE}
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.internal.Logging

private[spark] class CachedEventHubsReceiver(
    eventhubsParams: Map[String, String],
    partitionId: Int,
    startOffset: Long,
    offsetType: EventHubsOffsetType,
    maximumEventRate: Int,
    batchInterval: Long,
    currentTimestamp: Long) {

  def receive(expectedEventNum: Int): Iterable[EventData] = {
    val receiver = CachedEventHubsReceiver.getOrCreateReceiver(eventhubsParams,
      partitionId.toString, startOffset.toString, offsetType, maximumEventRate, batchInterval,
      currentTimestamp)
    val events = receiver.receive(math.min(expectedEventNum, receiver.getPrefetchCount)).get()
    if (events != null) events.asScala else null
  }
}


private[spark] object CachedEventHubsReceiver extends Logging {

  private var _eventhubsClient: AzureEventHubClient = _
  private var _receiverCache: Cache[EventHubNameAndPartition, (PartitionReceiver, Long)] = _

  private def createAndCacheReceiver(
      connectionString: String,
      consumerGroup: String,
      ehNameAndPartition: EventHubNameAndPartition,
      offsetType: EventHubsOffsetType,
      currentOffset: String,
      receiverEpoch: Long,
      currentTimestamp: Long,
      batchInterval: Long): PartitionReceiver = {
    implicit val batchIntervalImplicit = batchInterval
    val newReceiver = createNewReceiver(azureEventHubsClient(connectionString),
      consumerGroup, ehNameAndPartition.partitionId.toString, offsetType, currentOffset,
      receiverEpoch)
    receiverCache.put(ehNameAndPartition, (newReceiver, currentTimestamp))
    newReceiver
  }

  def getOrCreateReceiver(
      eventhubsParams: Predef.Map[String, String],
      partitionId: String,
      startOffset: String,
      offsetType: EventHubsOffsetType,
      maximumEventRate: Int,
      batchInterval: Long,
      currentTimestamp: Long): PartitionReceiver = {
    implicit val batchIntervalImplicit = batchInterval
    val (connectionString, consumerGroup, receiverEpoch) = configureGeneralParameters(
      eventhubsParams)
    val currentOffset = startOffset
    MAXIMUM_EVENT_RATE = configureMaxEventRate(maximumEventRate)
    val ehName = eventhubsParams("eventhubs.name")
    val ehNameAndPartition = EventHubNameAndPartition(ehName, partitionId.toInt)
    val receiverAndTimestamp = receiverCache.getIfPresent(ehNameAndPartition)
    if (receiverAndTimestamp != null) {
      val cachedReceiver = receiverAndTimestamp._1
      val ts = receiverAndTimestamp._2
      if (ts == currentTimestamp - batchInterval) {
        receiverCache.put(ehNameAndPartition, (cachedReceiver, currentTimestamp))
        cachedReceiver
      } else {
        logInfo(s"Cached receiver for $ehNameAndPartition is too old (timestamp: $ts)," +
          s" creating a new one")
        createAndCacheReceiver(connectionString, consumerGroup, ehNameAndPartition, offsetType,
          currentOffset, receiverEpoch, currentTimestamp, batchInterval)
      }
    } else {
      logInfo(s"missed receiverCache for $ehNameAndPartition at $currentTimestamp")
      createAndCacheReceiver(connectionString, consumerGroup, ehNameAndPartition, offsetType,
        currentOffset, receiverEpoch, currentTimestamp, batchInterval)
    }
  }

  def azureEventHubsClient(connectionString: String): AzureEventHubClient = synchronized {
    if (_eventhubsClient == null) {
      _eventhubsClient = AzureEventHubClient.createFromConnectionStringSync(connectionString)
    }
    _eventhubsClient
  }

  def receiverCache(implicit batchInterval: Long):
      Cache[EventHubNameAndPartition, (PartitionReceiver, Long)] = synchronized {
    if (_receiverCache == null) {
      _receiverCache = initReceiverCache(batchInterval)
    }
    _receiverCache
  }

  private def initReceiverCache(batchInterval: Long):
    Cache[EventHubNameAndPartition, (PartitionReceiver, Long)] = {
    CacheBuilder.newBuilder()
      .expireAfterWrite(batchInterval * 4, TimeUnit.MILLISECONDS)
      .removalListener(new RemovalListener[EventHubNameAndPartition, (PartitionReceiver, Long)]() {
        override def onRemoval(removal: RemovalNotification[EventHubNameAndPartition,
          (PartitionReceiver, Long)]) {
          val (receiver, _) = removal.getValue
          receiver.closeSync()
        }
      })
      .build[EventHubNameAndPartition, (PartitionReceiver, Long)]()
  }
}
