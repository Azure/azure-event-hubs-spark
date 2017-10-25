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

package org.apache.spark.eventhubs.common.utils

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient }
import org.apache.spark.eventhubs.common.NameAndPartition
import org.apache.spark.eventhubs.common.client.{ Client, EventHubsOffsetTypes }
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.streaming.StreamingContext

/**
 * TestClientSugar implements Client so all methods and variables are NOPs. This will
 * reduce repetitive code when the Client is mocked in testing.
 */
// TODO: consolidate these to one mock object/companion
class SimulatedEventHubsRestClient(eventHubs: SimulatedEventHubs)
    extends Client
    with TestClientSugar {

  override def lastOffsetAndSeqNo(
      targetEventHubNameAndPartition: NameAndPartition): Option[(Long, Long)] = {
    val x = eventHubs.messageStore(targetEventHubNameAndPartition).length.toLong - 1
    Some((x, x))
  }

  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): Option[Long] = {
    Some(
      eventHubs
        .messageStore(nameAndPartition)
        .last
        .getSystemProperties
        .getEnqueuedTime
        .toEpochMilli)
  }
}

class TestEventHubsClient(ehParams: Map[String, String],
                          eventHubs: SimulatedEventHubs,
                          latestRecords: Map[NameAndPartition, (Long, Long, Long)])
    extends Client
    with TestClientSugar {
  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = ehParams("eventhubs.name")
    if (offsetType != EventHubsOffsetTypes.EnqueueTime) {
      eventHubs.search(NameAndPartition(eventHubName, partitionId),
                       currentOffset.toInt,
                       expectedEventNum)
    } else {
      eventHubs.searchWithTime(NameAndPartition(eventHubName, partitionId),
                               ehParams("eventhubs.filter.enqueuetime").toLong,
                               expectedEventNum)
    }
  }

  override def lastOffsetAndSeqNo(nameAndPartition: NameAndPartition): Option[(Long, Long)] = {
    val (offset, seq, _) = latestRecords(nameAndPartition)
    Some(offset, seq)
  }

  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): Option[Long] =
    Some(latestRecords(nameAndPartition)._3)
}

class FluctuatedEventHubClient(ehParams: Map[String, String],
                               eventHubs: SimulatedEventHubs,
                               ssc: StreamingContext,
                               messagesBeforeEmpty: Long,
                               numBatchesBeforeNewData: Int,
                               latestRecords: Map[NameAndPartition, (Long, Long)])
    extends Client
    with TestClientSugar {
  private var callIndex = -1

  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = ehParams("eventhubs.name")
    if (offsetType != EventHubsOffsetTypes.EnqueueTime) {
      eventHubs.search(NameAndPartition(eventHubName, partitionId),
                       currentOffset.toInt,
                       expectedEventNum)
    } else {
      eventHubs.searchWithTime(NameAndPartition(eventHubName, partitionId),
                               ehParams("eventhubs.filter.enqueuetime").toLong,
                               expectedEventNum)
    }
  }

  override def lastOffsetAndSeqNo(nameAndPartition: NameAndPartition): Option[(Long, Long)] = {
    callIndex += 1
    if (callIndex < numBatchesBeforeNewData) {
      Some(messagesBeforeEmpty - 1, messagesBeforeEmpty - 1)
    } else {
      Some(latestRecords(nameAndPartition))
    }
  }

  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): Option[Long] = {
    Some(Long.MaxValue)
  }
}

sealed trait TestClientSugar extends Client {
  protected var partitionId: Int = _
  protected var offsetType: EventHubsOffsetType = _
  protected var currentOffset: String = _

  override private[spark] var client: EventHubClient = _

  override def close(): Unit = {}

  override def lastOffsetAndSeqNo(
      eventHubNameAndPartition: NameAndPartition): Option[(Long, Long)] =
    Option.empty

  override def initReceiver(partitionId: String,
                            offsetType: EventHubsOffsetType,
                            currentOffset: String): Unit = {
    this.partitionId = partitionId.toInt
    this.offsetType = offsetType
    this.currentOffset = currentOffset
  }

  override def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): Option[Long] =
    Option.empty

  override def receive(expectedEvents: Int): Iterable[EventData] = Iterable[EventData]()

  override def earliestSeqNo(eventHubNameAndPartition: NameAndPartition): Option[Long] =
    Some(-1L)
}
