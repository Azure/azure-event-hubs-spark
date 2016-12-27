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

package org.apache.spark.streaming.eventhubs.evenhubs

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.streaming.eventhubs.{EventHubClient, EventHubNameAndPartition, EventHubsClientWrapper}

class SimulatedEventHubs(
    namespace: String,
    messagesStore: Map[EventHubNameAndPartition, Array[EventData]]) extends Serializable {

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
    eventHubNameAndPartitions: List[EventHubNameAndPartition]) extends EventHubClient {

  private val latestRecords = eventHubNameAndPartitions.map(ehNameAndPartition =>
    (ehNameAndPartition, (Long.MaxValue, Long.MaxValue))).toMap

  override def endPointOfPartition():
  Option[Predef.Map[EventHubNameAndPartition, (Long, Long)]] = {
    Some(latestRecords)
  }

  override def close(): Unit = {}
}

