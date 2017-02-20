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

package org.apache.spark.streaming.eventhubs.utils

import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper}
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.streaming.StreamingContext

private[eventhubs] class SimulatedEventHubs(
    namespace: String,
    val messagesStore: Map[EventHubNameAndPartition, Array[EventData]]) extends Serializable {

  def search(ehPartition: EventHubNameAndPartition, offset: Int, eventNum: Int):
      List[EventData] = {
    val ret = new ListBuffer[EventData]
    for (i <- 0 until eventNum) {
      // as in eventhub, offset is exclusive
      val index = offset + i + 1
      if (index < messagesStore(ehPartition).length) {
        ret += messagesStore(ehPartition)(index)
      }
    }
    ret.toList
  }
}


private[eventhubs] class TestEventHubsReceiver(
    eventHubParameters: Map[String, String],
    eventHubs: SimulatedEventHubs,
    partitionId: Int,
    startOffset: Long)
  extends EventHubsClientWrapper {

  val eventHubName = eventHubParameters("eventhubs.name")

  override def receive(expectedEventNum: Int): Iterable[EventData] = {
    val eventHubName = eventHubParameters("eventhubs.name")
    eventHubs.search(EventHubNameAndPartition(eventHubName, partitionId), startOffset.toInt,
      expectedEventNum)
  }
}

private[eventhubs] class TestRestEventHubClient(
    latestRecords: Map[EventHubNameAndPartition, (Long, Long)]) extends EventHubClient {

  override def endPointOfPartition(
      retryIfFail: Boolean,
      targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List()):
    Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    Some(latestRecords)
  }

  override def close(): Unit = {}
}

private[eventhubs] class FragileEventHubClient private extends EventHubClient {

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

  override def close(): Unit = {}
}

// ugly stuff to make things checkpointable in tests
private[eventhubs] object FragileEventHubClient {

  var callIndex = -1
  var numBatchesBeforeCrashedEndpoint = 0
  var lastBatchWhenEndpointCrashed = 0
  var latestRecords: Map[EventHubNameAndPartition, (Long, Long)] = Map()

  def getInstance(eventHubNameSpace: String, eventhubsParams: Map[String, Map[String, String]]):
    FragileEventHubClient = {
    new FragileEventHubClient()
  }
}


private[eventhubs] class FluctuatedEventHubClient(
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
}

