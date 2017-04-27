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

import java.util.Calendar

import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventhubs.EventData.SystemProperties
import com.microsoft.azure.servicebus.amqp.AmqpConstants
import org.powermock.reflect.Whitebox

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.internal.Logging

private[spark] object EventHubsTestUtilities extends Logging {

  def simulateEventHubs[T, U](
     eventHubsParameters: Map[String, String],
     eventPayloadsAndProperties: Seq[(T, Seq[U])] = Seq.empty[(T, Seq[U])]): SimulatedEventHubs = {

    assert(eventHubsParameters != null)
    assert(eventHubsParameters.nonEmpty)

    // Round-robin allocation of payloads to partitions
    val eventHubsNamespace = eventHubsParameters("eventhubs.namespace")
    val eventHubsName = eventHubsParameters("eventhubs.name")
    val eventHubsPartitionList = {
      for (i <- 0 until eventHubsParameters("eventhubs.partition.count").toInt)
        yield EventHubNameAndPartition(eventHubsName, i)
    }
    val payloadPropertyStore = roundRobinAllocation(eventHubsPartitionList.map(x => x -> 0).toMap,
      eventPayloadsAndProperties)
    simulatedEventHubs = new SimulatedEventHubs(eventHubsNamespace, payloadPropertyStore)

    simulatedEventHubs
  }

  def getOrSimulateEventHubs[T, U](
      eventHubsParameters: Map[String, String],
      eventPayloadsAndProperties: Seq[(T, Seq[U])] = Seq.empty[(T, Seq[U])]): SimulatedEventHubs = {
    if (simulatedEventHubs == null) {
      simulatedEventHubs = simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties)
    }
    simulatedEventHubs
  }

  def getHighestOffsetPerPartition(eventHubs: SimulatedEventHubs):
      Map[EventHubNameAndPartition, (Long, Long)] = {
    eventHubs.messageStore.map {
      case (ehNameAndPartition, messageQueue) => (ehNameAndPartition,
        (messageQueue.length.toLong - 1, messageQueue.length.toLong - 1))
    }
  }

  def addEventsToEventHubs[T, U](
     eventHubs: SimulatedEventHubs,
     eventPayloadsAndProperties: Seq[(T, Seq[U])]): SimulatedEventHubs = {
    // Round-robin allocation of payloads to partitions
    val payloadPropertyStore = roundRobinAllocation(eventHubs.eventHubsNamedPartitions
      .map(x => x -> eventHubs.messageStore(x).length).toMap, eventPayloadsAndProperties)
    eventHubs.send(payloadPropertyStore)
    eventHubs
  }

  private def roundRobinAllocation[T, U](
      eventHubsPartitionOffsetMap: Map[EventHubNameAndPartition, Int],
      eventPayloadsAndProperties: Seq[(T, Seq[U])] = Seq.empty[(T, Seq[U])]):
    Map[EventHubNameAndPartition, Array[EventData]] = {

    val eventHubsPartitionList : Seq[EventHubNameAndPartition] =
      eventHubsPartitionOffsetMap.keys.toSeq

    if (eventPayloadsAndProperties.isEmpty) {
      eventHubsPartitionList.map(x => x -> Seq.empty[EventData].toArray).toMap
    } else {
      val eventAllocation: Seq[(EventHubNameAndPartition, Seq[(T, Seq[U])])] = {
        if (eventHubsPartitionList.length >= eventPayloadsAndProperties.length) {
          eventHubsPartitionList.zip(eventPayloadsAndProperties.map(x => Seq(x)))
        } else {
          eventPayloadsAndProperties.zipWithIndex
            .map(x => (eventHubsPartitionList(x._2 % eventHubsPartitionList.length), x._1)).
            groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) }
        }.toSeq
      }
      eventAllocation.map {
        case (eventHubNameAndPartition, payloadPropertyBag) =>
          (eventHubNameAndPartition, generateEventData(payloadPropertyBag,
            eventHubNameAndPartition.partitionId,
            eventHubsPartitionOffsetMap(eventHubNameAndPartition)))
      }.toMap
    }
  }

  private def generateEventData[T, U](
      payloadPropertyBag: Seq[(T, Seq[U])],
      partitionId: Int, startingOffset: Int): Array[EventData] = {
    var queueOffset = startingOffset
    var eventIndex = 0
    val eventDataArray = new Array[EventData](payloadPropertyBag.length)
    val publisherName = "Microsoft Corporation"

    for((payload, properties) <- payloadPropertyBag) {
      val eventData = new EventData(payload.toString.getBytes)
      val systemPropertiesMap = new java.util.HashMap[String, AnyRef]()
      systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME,
        queueOffset.toString)
      systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME,
        Long.box(queueOffset))
      systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME,
        partitionId.toString)
      systemPropertiesMap.put(AmqpConstants.PUBLISHER_ANNOTATION_NAME,
        publisherName.toString)
      systemPropertiesMap.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME,
        Calendar.getInstance().getTime)
      val systemProperties = new SystemProperties(systemPropertiesMap)
      Whitebox.setInternalState(eventData, "systemProperties", systemProperties.asInstanceOf[Any])
      for (property <- properties) {
        property match {
          case p@Tuple2(_, _) =>
            eventData.getProperties.put(p._1.toString, p._2.asInstanceOf[AnyRef])
          case _ =>
            eventData.getProperties.put("output", property.asInstanceOf[AnyRef])
        }
      }
      eventDataArray(eventIndex) = eventData
      queueOffset += 1
      eventIndex += 1
    }

    eventDataArray
  }

  private var simulatedEventHubs: SimulatedEventHubs = _
}