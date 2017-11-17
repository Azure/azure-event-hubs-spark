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
import org.apache.spark.eventhubs.common.{
  EnqueueTime,
  EventHubsConf,
  NameAndPartition,
  Offset,
  SequenceNumber
}
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

  override def latestSeqNo(targetEventHubNameAndPartition: Int): Any = {
    val x = eventHubs.messageStore(targetEventHubNameAndPartition).length.toLong - 1
    (x, x)
  }

  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): EnqueueTime = {
    eventHubs
      .messageStore(nameAndPartition)
      .last
      .getSystemProperties
      .getEnqueuedTime
      .toEpochMilli
  }
}

class TestEventHubsClient(ehConf: EventHubsConf,
                          eventHubs: SimulatedEventHubs,
                          latestRecords: Map[NameAndPartition, (Long, Long, Long)])
    extends Client
    with TestClientSugar {
  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = ehConf.name.get
    if (offsetType != EventHubsOffsetTypes.EnqueueTime) {
      eventHubs.search(NameAndPartition(eventHubName, partitionId),
                       currentOffset.toInt,
                       expectedEventNum)
    } else {
      eventHubs.searchWithTime(NameAndPartition(eventHubName, partitionId),
                               ehConf.startEnqueueTimes.head._2,
                               expectedEventNum)
    }
  }

  override def latestSeqNo(nameAndPartition: Int): Any = {
    if (latestRecords != null) {
      val (offset, seq, _) = latestRecords(nameAndPartition)
      (offset, seq)
    } else {
      val x = eventHubs.messageStore(nameAndPartition).length.toLong - 1
      (x, x)
    }
  }

  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): EnqueueTime = {
    if (latestRecords != null) {
      latestRecords(nameAndPartition)._3
    } else {
      eventHubs
        .messageStore(nameAndPartition)
        .last
        .getSystemProperties
        .getEnqueuedTime
        .toEpochMilli
    }
  }
}

class FluctuatedEventHubClient(ehConf: EventHubsConf,
                               eventHubs: SimulatedEventHubs,
                               ssc: StreamingContext,
                               messagesBeforeEmpty: Long,
                               numBatchesBeforeNewData: Int,
                               latestRecords: Map[NameAndPartition, (Long, Long)])
    extends Client
    with TestClientSugar {
  private var callIndex = -1

  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = ehConf.name.get
    if (offsetType != EventHubsOffsetTypes.EnqueueTime) {
      eventHubs.search(NameAndPartition(eventHubName, partitionId),
                       currentOffset.toInt,
                       expectedEventNum)
    } else {
      eventHubs.searchWithTime(NameAndPartition(eventHubName, partitionId),
                               ehConf.startEnqueueTimes.head._2,
                               expectedEventNum)
    }
  }

  override def latestSeqNo(nameAndPartition: Int): Any = {
    callIndex += 1
    if (callIndex < numBatchesBeforeNewData) {
      (messagesBeforeEmpty - 1, messagesBeforeEmpty - 1)
    } else {
      latestRecords(nameAndPartition)
    }
  }

  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): EnqueueTime = {
    Long.MaxValue
  }
}

sealed trait TestClientSugar extends Client {
  import org.apache.spark.eventhubs.common.{ PartitionId, Offset, SequenceNumber }
  protected var partitionId: Int = _
  protected var offsetType: EventHubsOffsetType = _
  protected var currentOffset: String = _

  override private[spark] var client: EventHubClient = _

  override def close(): Unit = {}

  override def latestSeqNo(partitionId: PartitionId): Any =
    null

  override def initReceiver(partitionId: String,
                            offsetType: EventHubsOffsetType,
                            currentOffset: String): Unit = {
    this.partitionId = partitionId.toInt
    this.offsetType = offsetType
    this.currentOffset = currentOffset
  }

  override def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): EnqueueTime =
    Long.MaxValue

  override def receive(expectedEvents: Int): Iterable[EventData] = Iterable[EventData]()

  override def earliestSeqNo(eventHubNameAndPartition: NameAndPartition): SequenceNumber =
    -1L

  override def translate[T](ehConf: EventHubsConf): Map[PartitionId, SequenceNumber] =
    Map.empty
}
