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

import java.time.Instant

import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventhubs.EventData.SystemProperties
import com.microsoft.azure.servicebus.amqp.AmqpConstants

import scala.reflect.ClassTag
import org.powermock.reflect.Whitebox

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.internal.Logging

object EventHubsTestUtilities extends Logging {

  def simulateEventHubs
  [T: ClassTag, U: ClassTag](eventHubsNamespace: String,
                             eventHubsParameters: Map[String, Map[String, String]],
                             eventPayloadsAndProperties: Seq[(T, Seq[U])]):
  SimulatedEventHubs = {

    // Round-robin allocation of payloads to partitions

    val payloadPropertyStore = eventHubsParameters.keys.flatMap {
      eventHubName =>

        val eventHubsPartitionList = {
          for (i <- 0 until eventHubsParameters(eventHubName)("eventhubs.partition.count").toInt)
            yield EventHubNameAndPartition(eventHubName, i)
        }.toArray

        if (eventHubsPartitionList.length >= eventPayloadsAndProperties.length) {
          eventHubsPartitionList.zip(eventPayloadsAndProperties.map(x => Seq(x)))
        }
        else {

          eventPayloadsAndProperties.zipWithIndex
            .map( x => (x._1, eventHubsPartitionList(x._2 % eventHubsPartitionList.length)))
            .map(x => (x._2, x._1)).groupBy(_._1).mapValues(z => z.map(_._2))
        }
    }.toMap

    new SimulatedEventHubs(eventHubsNamespace, payloadPropertyStore.map {
      case (eventHubNameAndPartition, payloadPropertyBag) =>
        (eventHubNameAndPartition,
          generateEventData(payloadPropertyBag, eventHubNameAndPartition.partitionId))
    })
  }

  def addEventsToEventHubs
  [T: ClassTag, U: ClassTag](
                              eventHubs: SimulatedEventHubs,
                              eventPayloadsAndProperties: Seq[(T, Seq[U])]) : Unit = {

    val eventHubsPartitionList = eventHubs.eventHubsNamedPartitions

    // Round-robin allocation of payloads to partitions

    val payloadPropertyStore = {

      if (eventHubsPartitionList.length >= eventPayloadsAndProperties.length) {
        eventHubsPartitionList.zip(eventPayloadsAndProperties.map(x => Seq(x)))
      }
      else {
        eventPayloadsAndProperties.zipWithIndex
          .map( x => (x._1, eventHubsPartitionList(x._2 % eventHubsPartitionList.length)))
          .map(x => (x._2, x._1)).groupBy(_._1).mapValues(z => z.map(_._2))
      }
    }.map {
      case (eventHubNameAndPartition, payloadPropertyBag) =>
        (eventHubNameAndPartition,
          generateEventData(payloadPropertyBag, eventHubNameAndPartition.partitionId))
    }.toMap

    eventHubs.send(payloadPropertyStore)
  }

  private def generateEventData
  [T: ClassTag, U: ClassTag](
                              payloadPropertyBag: Seq[(T, Seq[U])],
                              partitionId: Int): Array[EventData] = {

    var offsetSetInQueue = 0

    val eventDataArray = new Array[EventData](payloadPropertyBag.length)

    for((payload, properties) <- payloadPropertyBag) {

      val eventData = new EventData(payload.toString.getBytes)

      for (property <- properties) {

        val systemPropertiesMap = new java.util.HashMap[String, AnyRef]()
        systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME,
          offsetSetInQueue.toString)
        systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME,
          Long.box(offsetSetInQueue))
        systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME,
          Int.box(partitionId))
        systemPropertiesMap.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME, Instant.now())

        val systemProperties = new SystemProperties(systemPropertiesMap)
        Whitebox.setInternalState(eventData, "systemProperties", systemProperties.asInstanceOf[Any])
        property match {
          case p@Tuple2(_, _) =>
            eventData.getProperties.put(p._1.toString, p._2.asInstanceOf[AnyRef])
          case _ =>
            eventData.getProperties.put("output", property.asInstanceOf[AnyRef])
        }
      }

      eventDataArray(offsetSetInQueue) = eventData

      offsetSetInQueue += 1
    }

    eventDataArray
  }
}