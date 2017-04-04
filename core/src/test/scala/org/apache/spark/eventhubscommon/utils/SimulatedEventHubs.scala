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

import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper, EventHubsOffsetTypes}
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.streaming.StreamingContext

class SimulatedEventHubs(
    eventHubsNamespace: String,
    initialData: Map[EventHubNameAndPartition, Array[EventData]]) extends Serializable {

  assert(initialData != null)

  var messageStore: Map[EventHubNameAndPartition, Array[EventData]] = initialData
  val eventHubsNamedPartitions: Seq[EventHubNameAndPartition] = initialData.keys.toSeq

  def searchWithTime(
      eventHubsNamedPartition: EventHubNameAndPartition,
      enqueueTime: Long,
      eventCount: Int): List[EventData] = {
    val resultData = new ListBuffer[EventData]
    for (msg <- messageStore(eventHubsNamedPartition)) {
      if (resultData.length >= eventCount) {
        return resultData.toList
      }
      if (msg.getSystemProperties.getEnqueuedTime.toEpochMilli >= enqueueTime) {
        resultData += msg
      }
    }
    resultData.toList
  }

  def search(eventHubsNamedPartition: EventHubNameAndPartition, eventOffset: Int, eventCount: Int):
      List[EventData] = {
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
      (messageStore.toSeq ++ newData.toSeq).groupBy(_._1)
        .map{case (k, v) => (k, v.flatMap(_._2).toArray)}
    messageStore = combinedData
  }
}

class TestEventHubsReceiver(
    eventHubParameters: Map[String, String],
    eventHubs: SimulatedEventHubs,
    partitionId: Int,
    startOffset: Long,
    offsetType: EventHubsOffsetType)
  extends EventHubsClientWrapper {

  val eventHubName = eventHubParameters("eventhubs.name")

  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = eventHubParameters("eventhubs.name")
    if (offsetType != EventHubsOffsetTypes.InputTimeOffset) {
      eventHubs.search(EventHubNameAndPartition(eventHubName, partitionId), startOffset.toInt,
        expectedEventNum)
    } else {
      eventHubs.searchWithTime(EventHubNameAndPartition(eventHubName, partitionId),
        eventHubParameters("eventhubs.filter.enqueuetime").toLong, expectedEventNum)
    }
  }
}

class SimulatedEventHubsRestClient(eventHubs: SimulatedEventHubs) extends EventHubClient {

  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List()):
    Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    Some(eventHubs.messageStore
      .map(x => x._1 -> (x._2.length.toLong - 1, x._2.length.toLong - 1)))
  }

  override def close(): Unit = {}
}

class TestRestEventHubClient(
    latestRecords: Map[EventHubNameAndPartition, (Long, Long)]) extends EventHubClient {

  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List()):
    Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    Some(latestRecords)
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, Long]] = {
    throw new UnsupportedOperationException("lastEnqueueTimeOfPartitions is not supported by this" +
      " client yet, please use RestfulEventHubClient")
  }

  override def close(): Unit = {}
}

class FragileEventHubClient private extends EventHubClient {

  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List()):
    Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    import FragileEventHubClient._

    callIndex += 1
    if (callIndex < numBatchesBeforeCrashedEndpoint) {
      Some(latestRecords)
    } else if (callIndex < lastBatchWhenEndpointCrashed) {
      None
    } else {
      Some(latestRecords)
    }
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, Long]] = {
    throw new UnsupportedOperationException("lastEnqueueTimeOfPartitions is not supported by this" +
      " client yet, please use RestfulEventHubClient")
  }

  override def close(): Unit = {}
}

// ugly stuff to make things checkpointable in tests
object FragileEventHubClient {

  var callIndex = -1
  var numBatchesBeforeCrashedEndpoint = 0
  var lastBatchWhenEndpointCrashed = 0
  var latestRecords: Map[EventHubNameAndPartition, (Long, Long)] = Map()

  def getInstance(eventHubNameSpace: String, eventhubsParams: Map[String, Map[String, String]]):
    FragileEventHubClient = {
    new FragileEventHubClient()
  }
}


class FluctuatedEventHubClient(
    ssc: StreamingContext,
    messagesBeforeEmpty: Long,
    numBatchesBeforeNewData: Int,
    latestRecords: Map[EventHubNameAndPartition, (Long, Long)]) extends EventHubClient {

  private var callIndex = -1

  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List()):
    Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    callIndex += 1
    if (callIndex < numBatchesBeforeNewData) {
      Some(latestRecords.map{
        case (ehNameAndPartition, _) =>
          (ehNameAndPartition, (messagesBeforeEmpty - 1, messagesBeforeEmpty - 1))
      })
    } else {
      Some(latestRecords)
    }
  }

  override def close(): Unit = {}

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueueTimeOfPartitions(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition]):
    Option[Map[EventHubNameAndPartition, Long]] = {
    throw new UnsupportedOperationException("lastEnqueueTimeOfPartitions is not supported by this" +
      " client yet, please use RestfulEventHubClient")
  }
}

