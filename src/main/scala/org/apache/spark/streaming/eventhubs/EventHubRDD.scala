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

package org.apache.spark.streaming.eventhubs

// scalastyle:off
import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.EventData
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.eventhubs.checkpoint.{OffsetRange, OffsetStoreParams, ProgressWriter}
import org.apache.spark.streaming.Time
// scalastyle:on

private[eventhubs] class EventHubRDDPartition(
    val sparkPartitionId: Int,
    val eventHubNameAndPartitionID: EventHubNameAndPartition,
    val fromOffset: Long,
    val fromSeq: Long,
    val untilSeq: Long) extends Partition {

  override def index: Int = sparkPartitionId
}

class EventHubRDD(
    sc: SparkContext,
    eventHubsParamsMap: Map[String, Map[String, String]],
    val offsetRanges: List[OffsetRange],
    batchTime: Time,
    offsetParams: OffsetStoreParams,
    eventHubReceiverCreator: (Map[String, String], Int, Long, Int) => EventHubsClientWrapper)
  extends RDD[EventData](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (offsetRange, index) =>
      new EventHubRDDPartition(index, offsetRange.eventHubNameAndPartition, offsetRange.fromOffset,
        offsetRange.fromSeq, offsetRange.untilSeq)
    }.toArray
  }

  private def wrappingReceive(eventHubNameAndPartition: EventHubNameAndPartition,
                              eventHubClient: EventHubsClientWrapper, expectedEventNumber: Int):
    List[EventData] = {
    val receivedBuffer = new ListBuffer[EventData]
    val receivingTrace = new ListBuffer[Long]
    var cnt = 0
    while (receivedBuffer.size < expectedEventNumber) {
      if (cnt > expectedEventNumber * 2) {
        throw new Exception(s"$eventHubNameAndPartition cannot return data, the trace is" +
          s" ${receivingTrace.toList}")
      }
      val receivedEvents = eventHubClient.receive(expectedEventNumber - receivedBuffer.size).
        toList
      receivingTrace += receivedEvents.length
      cnt += 1
      receivedBuffer ++= receivedEvents
    }
    receivedBuffer.toList
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[EventData] = {
    val eventHubPartition = split.asInstanceOf[EventHubRDDPartition]
    val progressWriter = new ProgressWriter(offsetParams.checkpointDir,
      offsetParams.appName, offsetParams.streamId, offsetParams.eventHubNamespace,
      eventHubPartition.eventHubNameAndPartitionID, batchTime.milliseconds, new Configuration())
    val fromOffset = eventHubPartition.fromOffset
    if (eventHubPartition.fromSeq >= eventHubPartition.untilSeq) {
      logInfo(s"No new data in ${eventHubPartition.eventHubNameAndPartitionID}")
      progressWriter.write(batchTime.milliseconds, eventHubPartition.fromOffset,
        eventHubPartition.fromSeq)
      logInfo(s"write offset $fromOffset, sequence number" +
        s" ${eventHubPartition.fromSeq} for EventHub" +
        s" ${eventHubPartition.eventHubNameAndPartitionID}")
      Iterator()
    } else {
      val maxRate = (eventHubPartition.untilSeq - eventHubPartition.fromSeq).toInt
      logInfo(s"${eventHubPartition.eventHubNameAndPartitionID}" +
        s" expected rate $maxRate, fromSeq ${eventHubPartition.fromSeq} untilSeq" +
        s" ${eventHubPartition.untilSeq}")
      val eventHubParameters = eventHubsParamsMap(eventHubPartition.eventHubNameAndPartitionID.
        eventHubName)
      val eventHubClient = eventHubReceiverCreator(eventHubParameters,
        eventHubPartition.eventHubNameAndPartitionID.partitionId, fromOffset, maxRate)
      val receivedEvents = wrappingReceive(eventHubPartition.eventHubNameAndPartitionID,
        eventHubClient, maxRate)
      logInfo(s"${eventHubPartition.eventHubNameAndPartitionID} received ${receivedEvents.size}" +
        s" events")
      val lastEvent = receivedEvents.last
      val endOffset = lastEvent.getSystemProperties.getOffset.toLong
      progressWriter.write(batchTime.milliseconds, endOffset,
        lastEvent.getSystemProperties.getSequenceNumber)
      logInfo(s"write offset $endOffset, sequence number" +
        s" ${lastEvent.getSystemProperties.getSequenceNumber} for EventHub" +
        s" ${eventHubPartition.eventHubNameAndPartitionID}")
      eventHubClient.close()
      receivedEvents.iterator
    }
  }
}
