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

package org.apache.spark.eventhubscommon.utils

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubscommon.client.{ Client, EventHubsOffsetTypes }
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable.ListBuffer

class SimulatedEventHubs(eventHubsNamespace: String,
                         initialData: Map[EventHubNameAndPartition, Array[EventData]])
    extends Serializable {

  assert(initialData != null)

  var messageStore: Map[EventHubNameAndPartition, Array[EventData]] = initialData
  val eventHubsNamedPartitions: Seq[EventHubNameAndPartition] = initialData.keys.toSeq

  def searchWithTime(eventHubsNamedPartition: EventHubNameAndPartition,
                     enqueueTime: Long,
                     eventCount: Int): List[EventData] = {
    val resultData = new ListBuffer[EventData]
    for (msg <- messageStore(eventHubsNamedPartition)) {
      if (resultData.length >= eventCount) {
        return resultData.toList
      }
      if (msg.getSystemProperties.getEnqueuedTime.getEpochSecond >= enqueueTime) {
        resultData += msg
      }
    }
    resultData.toList
  }

  def search(eventHubsNamedPartition: EventHubNameAndPartition,
             eventOffset: Int,
             eventCount: Int): List[EventData] = {
    val resultData = new ListBuffer[EventData]
    for (i <- 0 until eventCount) {
      // as in eventhub, offset is exclusive
      val messageIndex = eventOffset + i + 1
      if (messageIndex < messageStore(eventHubsNamedPartition).length) {
        resultData += messageStore(eventHubsNamedPartition)(messageIndex)
      }
    }
    resultData.toList
  }

  def send(newData: Map[EventHubNameAndPartition, Array[EventData]]): Unit = {
    val combinedData: Map[EventHubNameAndPartition, Array[EventData]] =
      (messageStore.toSeq ++ newData.toSeq)
        .groupBy(_._1)
        .map { case (k, v) => (k, v.flatMap(_._2).toArray) }
    messageStore = combinedData
  }
}

// TODO: consolidate these to one mock object/companion
class SimulatedEventHubsRestClient(eventHubs: SimulatedEventHubs)
    extends Client
    with TestClientSugar {

  override def endPointOfPartition(
      targetEventHubNameAndPartition: EventHubNameAndPartition): Option[(Long, Long)] = {
    val x = eventHubs.messageStore(targetEventHubNameAndPartition).length.toLong - 1
    Some((x, x))
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      nameAndPartition: EventHubNameAndPartition): Option[Long] = {
    Some(
      eventHubs
        .messageStore(nameAndPartition)
        .last
        .getSystemProperties
        .getEnqueuedTime
        .toEpochMilli)
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def startSeqOfPartition(nameAndPartition: EventHubNameAndPartition): Option[Long] = {
    Some(-1L)
  }
}

class TestEventHubsClient(ehParams: Map[String, String],
                          eventHubs: SimulatedEventHubs,
                          latestRecords: Map[EventHubNameAndPartition, (Long, Long, Long)])
    extends Client
    with TestClientSugar {
  private var partitionId: Int = _
  private var offsetType: EventHubsOffsetType = _
  private var currentOffset: String = _

  override def initReceiver(partitionId: String,
                            offsetType: EventHubsOffsetType,
                            currentOffset: String): Unit = {
    this.partitionId = partitionId.toInt
    this.offsetType = offsetType
    this.currentOffset = currentOffset
  }

  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = ehParams("eventhubs.name")
    if (offsetType != EventHubsOffsetTypes.EnqueueTime) {
      eventHubs.search(EventHubNameAndPartition(eventHubName, partitionId),
                       currentOffset.toInt,
                       expectedEventNum)
    } else {
      eventHubs.searchWithTime(EventHubNameAndPartition(eventHubName, partitionId),
                               ehParams("eventhubs.filter.enqueuetime").toLong,
                               expectedEventNum)
    }
  }

  override def endPointOfPartition(
      nameAndPartition: EventHubNameAndPartition): Option[(Long, Long)] = {
    val (offset, seq, _) = latestRecords(nameAndPartition)
    Some(offset, seq)
  }

  override def lastEnqueueTimeOfPartitions(
      nameAndPartition: EventHubNameAndPartition): Option[Long] =
    Some(latestRecords(nameAndPartition)._3)

  override def startSeqOfPartition(nameAndPartition: EventHubNameAndPartition): Option[Long] =
    Some(-1L)
}

class FluctuatedEventHubClient(ehParams: Map[String, String],
                               eventHubs: SimulatedEventHubs,
                               ssc: StreamingContext,
                               messagesBeforeEmpty: Long,
                               numBatchesBeforeNewData: Int,
                               latestRecords: Map[EventHubNameAndPartition, (Long, Long)])
    extends Client
    with TestClientSugar {
  private var callIndex = -1
  private var partitionId: Int = _
  private var offsetType: EventHubsOffsetType = _
  private var currentOffset: String = _

  override def initReceiver(partitionId: String,
                            offsetType: EventHubsOffsetType,
                            currentOffset: String): Unit = {
    this.partitionId = partitionId.toInt
    this.offsetType = offsetType
    this.currentOffset = currentOffset
  }

  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = ehParams("eventhubs.name")
    if (offsetType != EventHubsOffsetTypes.EnqueueTime) {
      eventHubs.search(EventHubNameAndPartition(eventHubName, partitionId),
                       currentOffset.toInt,
                       expectedEventNum)
    } else {
      eventHubs.searchWithTime(EventHubNameAndPartition(eventHubName, partitionId),
                               ehParams("eventhubs.filter.enqueuetime").toLong,
                               expectedEventNum)
    }
  }

  override def endPointOfPartition(
      nameAndPartition: EventHubNameAndPartition): Option[(Long, Long)] = {
    callIndex += 1
    if (callIndex < numBatchesBeforeNewData) {
      Some(messagesBeforeEmpty - 1, messagesBeforeEmpty - 1)
    } else {
      Some(latestRecords(nameAndPartition))
    }
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      nameAndPartition: EventHubNameAndPartition): Option[Long] = {
    Some(Long.MaxValue)
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def startSeqOfPartition(nameAndPartition: EventHubNameAndPartition): Option[Long] = {
    Some(-1L)
  }
}
