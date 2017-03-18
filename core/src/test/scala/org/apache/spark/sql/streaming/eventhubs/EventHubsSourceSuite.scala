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

package org.apache.spark.sql.streaming.eventhubs

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.time.SpanSugar._
import scala.reflect.ClassTag

import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventhubs.EventData.SystemProperties
import com.microsoft.azure.servicebus.amqp.AmqpConstants

import org.powermock.reflect.Whitebox

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.utils.{SimulatedEventHubs, TestEventHubsReceiver, TestRestEventHubClient}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamTest}
import org.apache.spark.sql.test.{SharedSQLContext, TestSparkSession}
import org.apache.spark.util.Utils

abstract class EventHubsSourceTest extends StreamTest with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
      super.afterAll()
  }

  override val streamingTimeout = 30.seconds
}

class EventHubsSourceSuite extends EventHubsSourceTest {

  private val topicId = new AtomicInteger(0)

  testWithUninterruptibleThread("Expected sequence number is correct") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"10"
    )

    val eventPayloadsAndProperties = Seq(
      1 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d",
        "propertyE" -> "e", "propertyF" -> "f"),
      0 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j",
        "propertyK" -> "k"),
      3 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o", "propertyP" -> "p"),
      9 -> Seq("propertyQ" -> "q", "propertyR" -> "r", "propertyS" -> "s"),
      5 -> Seq("propertyT" -> "t", "propertyU" -> "u"),
      7 -> Seq("propertyV" -> "v")
    )

    val eventHubs = simulateEventHubs("ns1", eventPayloadsAndProperties,
      Map("eh1" -> eventHubsParameters))

    val highestOffsetPerEventHubs = eventHubs.messageStore.map {
      case (ehNameAndPartition, messageQueue) => (ehNameAndPartition,
        (messageQueue.length.toLong - 1, messageQueue.length.toLong - 1))
    }

    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubParams: Map[String, String], partitionId: Int, startOffset: Long, _: Int) =>
        new TestEventHubsReceiver(eventHubParams, eventHubs, partitionId, startOffset),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerEventHubs))

    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]

    assert(offset.batchId == 0)
    offset.targetSeqNums.values.foreach(x => assert(x == 2))
  }

  private def simulateEventHubs
  [T: ClassTag, U: ClassTag](
                 namespace: String,
                 eventPayloadsAndProperties: Seq[(T, Seq[U])],
                 eventhubsParams: Map[String, Map[String, String]]): SimulatedEventHubs = {

    val payloadPropertyStore = eventhubsParams.keys.flatMap {
      eventHubName =>

        val eventHubsPartitionList = {
          for (i <- 0 until eventhubsParams(eventHubName)("eventhubs.partition.count").toInt)
            yield EventHubNameAndPartition(eventHubName, i)
        }.toArray

        // Round-robin allocation of payloads to partitions

        if (eventHubsPartitionList.length >= eventPayloadsAndProperties.length) {
          eventHubsPartitionList.zip(eventPayloadsAndProperties.map(x => Seq(x)))
        }
        else {

          eventPayloadsAndProperties.zipWithIndex
            .map( x => (x._1, eventHubsPartitionList(x._2 % eventHubsPartitionList.length)))
            .map(x => (x._2, x._1)).groupBy(_._1).mapValues(z => z.map(_._2))
        }
      }.toMap

      new SimulatedEventHubs(namespace, payloadPropertyStore.map {
        case (eventHubNameAndPartition, payloadPropertyBag) =>
          (eventHubNameAndPartition,
            generateEventData(payloadPropertyBag, eventHubNameAndPartition.partitionId))
      })
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