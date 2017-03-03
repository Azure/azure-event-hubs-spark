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

package org.apache.spark.eventhubscommon.rdd

// scalastyle:off
import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.EventData
import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.eventhubscommon.client.EventHubsClientWrapper
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.progress.ProgressWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.{Partition, SparkContext, TaskContext}
// scalastyle:on

private class EventHubRDDPartition(
    val sparkPartitionId: Int,
    val eventHubNameAndPartitionID: EventHubNameAndPartition,
    val fromOffset: Long,
    val fromSeq: Long,
    val untilSeq: Long) extends Partition {

  override def index: Int = sparkPartitionId
}

private[spark] class EventHubsRDD(
    sc: SparkContext,
    eventHubsParamsMap: Map[String, Map[String, String]],
    val offsetRanges: List[OffsetRange],
    batchTime: Long,
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
    val progressWriter = new ProgressWriter(offsetParams.streamId, offsetParams.uid,
      eventHubPartition.eventHubNameAndPartitionID, batchTime, new Configuration(),
      offsetParams.checkpointDir, offsetParams.subDirs: _*)
    val fromOffset = eventHubPartition.fromOffset
    if (eventHubPartition.fromSeq >= eventHubPartition.untilSeq) {
      logInfo(s"No new data in ${eventHubPartition.eventHubNameAndPartitionID} at $batchTime")
      progressWriter.write(batchTime, eventHubPartition.fromOffset,
        eventHubPartition.fromSeq)
      logInfo(s"write offset $fromOffset, sequence number" +
        s" ${eventHubPartition.fromSeq} for EventHub" +
        s" ${eventHubPartition.eventHubNameAndPartitionID} at $batchTime")
      Iterator()
    } else {
      val maxRate = (eventHubPartition.untilSeq - eventHubPartition.fromSeq).toInt
      logInfo(s"${eventHubPartition.eventHubNameAndPartitionID}" +
        s" expected rate $maxRate, fromSeq ${eventHubPartition.fromSeq} (exclusive) untilSeq" +
        s" ${eventHubPartition.untilSeq} (inclusive) at $batchTime")
      val eventHubParameters = eventHubsParamsMap(eventHubPartition.eventHubNameAndPartitionID.
        eventHubName)
      val eventHubReceiver = eventHubReceiverCreator(eventHubParameters,
        eventHubPartition.eventHubNameAndPartitionID.partitionId, fromOffset, maxRate)
      val receivedEvents = wrappingReceive(eventHubPartition.eventHubNameAndPartitionID,
        eventHubReceiver, maxRate)
      val lastEvent = receivedEvents.last
      val endOffset = lastEvent.getSystemProperties.getOffset.toLong
      progressWriter.write(batchTime, endOffset,
        lastEvent.getSystemProperties.getSequenceNumber)
      logInfo(s"write offset $endOffset, sequence number" +
        s" ${lastEvent.getSystemProperties.getSequenceNumber} for EventHub" +
        s" ${eventHubPartition.eventHubNameAndPartitionID} at $batchTime")
      eventHubReceiver.close()
      receivedEvents.iterator
    }
  }
}

