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

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.common.NameAndPartition

import scala.collection.mutable.ListBuffer

class FooSimulatedEventHubs(eventHubsNamespace: String,
                            initialData: Map[NameAndPartition, Array[EventData]])
    extends Serializable {

  assert(initialData != null)

  var messageStore: Map[NameAndPartition, Array[EventData]] = initialData
  val eventHubsNamedPartitions: Seq[NameAndPartition] = initialData.keys.toSeq

  def searchWithTime(eventHubsNamedPartition: NameAndPartition,
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

  def search(eventHubsNamedPartition: NameAndPartition,
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

  def send(newData: Map[NameAndPartition, Array[EventData]]): Unit = {
    val combinedData: Map[NameAndPartition, Array[EventData]] =
      (messageStore.toSeq ++ newData.toSeq)
        .groupBy(_._1)
        .map { case (k, v) => (k, v.flatMap(_._2).toArray) }
    messageStore = combinedData
  }
}
