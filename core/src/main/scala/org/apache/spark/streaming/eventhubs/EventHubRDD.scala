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
import org.apache.spark.streaming.eventhubs.EventHubsOffsetTypes.EventHubsOffsetType
// scalastyle:on

private[eventhubs] class EventHubRDDPartition(
    val sparkPartitionId: Int,
    val eventHubNameAndPartitionID: EventHubNameAndPartition,
    val fromOffset: Long,
    val fromSeq: Long,
    val untilSeq: Long,
    val offsetType: EventHubsOffsetType) extends Partition {

  override def index: Int = sparkPartitionId
}

class EventHubRDD(
    sc: SparkContext,
    eventHubsParamsMap: Map[String, Map[String, String]],
    val offsetRanges: List[OffsetRange],
    batchTime: Time,
    offsetParams: OffsetStoreParams,
    eventHubReceiverCreator: (Map[String, String], Int, Long, EventHubsOffsetType, Int) =>
      EventHubsClientWrapper)
  extends RDD[EventData](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (offsetRange, index) =>
      new EventHubRDDPartition(index, offsetRange.eventHubNameAndPartition,
        offsetRange.fromOffset,
        offsetRange.fromSeq, offsetRange.untilSeq, offsetRange.offsetType)
    }.toArray
  }

  private def wrappingReceive(
      eventHubNameAndPartition: EventHubNameAndPartition,
      eventHubClient: EventHubsClientWrapper,
      expectedEventNumber: Int): List[EventData] = {
    val receivedBuffer = new ListBuffer[EventData]
    val receivingTrace = new ListBuffer[Long]
    var cnt = 0
    while (receivedBuffer.size < expectedEventNumber) {
      if (cnt > expectedEventNumber * 2) {
        throw new Exception(s"$eventHubNameAndPartition cannot return data, the trace is" +
          s" ${receivingTrace.toList}")
      }
      val receivedEventsItr = eventHubClient.receive(expectedEventNumber - receivedBuffer.size)
      if (receivedEventsItr == null) {
        // no more messages
        return receivedBuffer.toList
      }
      val receivedEvents = receivedEventsItr.toList
      receivingTrace += receivedEvents.length
      cnt += 1
      receivedBuffer ++= receivedEvents
    }
    receivedBuffer.toList
  }
  private def processFullyConsumedPartition(
      ehRDDPartition: EventHubRDDPartition, progressWriter: ProgressWriter): Iterator[EventData] = {
    logInfo(s"No new data in ${ehRDDPartition.eventHubNameAndPartitionID} at $batchTime")
    val fromOffset = ehRDDPartition.fromOffset
    progressWriter.write(batchTime.milliseconds, ehRDDPartition.fromOffset,
      ehRDDPartition.fromSeq)
    logInfo(s"write offset $fromOffset, sequence number" +
      s" ${ehRDDPartition.fromSeq} for EventHub" +
      s" ${ehRDDPartition.eventHubNameAndPartitionID} at $batchTime")
    Iterator()
  }

  private def retrieveDataFromPartition(
      ehRDDPartition: EventHubRDDPartition, progressWriter: ProgressWriter): Iterator[EventData] = {
    val fromOffset = ehRDDPartition.fromOffset
    val maxRate = (ehRDDPartition.untilSeq - ehRDDPartition.fromSeq).toInt
    val startTime = System.currentTimeMillis()
    logInfo(s"${ehRDDPartition.eventHubNameAndPartitionID}" +
      s" expected rate $maxRate, fromSeq ${ehRDDPartition.fromSeq} (exclusive) untilSeq" +
      s" ${ehRDDPartition.untilSeq} (inclusive) at $batchTime")
    var eventHubReceiver: EventHubsClientWrapper = null
    try {
      val eventHubParameters = eventHubsParamsMap(ehRDDPartition.eventHubNameAndPartitionID.
        eventHubName)
      eventHubReceiver = eventHubReceiverCreator(eventHubParameters,
        ehRDDPartition.eventHubNameAndPartitionID.partitionId, fromOffset,
        ehRDDPartition.offsetType, maxRate)
      val receivedEvents = wrappingReceive(ehRDDPartition.eventHubNameAndPartitionID,
        eventHubReceiver, maxRate)
      logInfo(s"received ${receivedEvents.length} messages before Event Hubs server indicates" +
        s" there is no more messages, time cost:" +
        s" ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")
      val lastEvent = receivedEvents.last
      val endOffset = lastEvent.getSystemProperties.getOffset.toLong
      val endSeq = lastEvent.getSystemProperties.getSequenceNumber
      progressWriter.write(batchTime.milliseconds, endOffset, endSeq)
      logInfo(s"write offset $endOffset, sequence number $endSeq for EventHub" +
        s" ${ehRDDPartition.eventHubNameAndPartitionID} at $batchTime")
      receivedEvents.iterator
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    } finally {
      if (eventHubReceiver != null) {
        eventHubReceiver.close()
      }
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[EventData] = {
    val ehRDDPartition = split.asInstanceOf[EventHubRDDPartition]
    val progressWriter = new ProgressWriter(offsetParams.checkpointDir,
      offsetParams.appName, offsetParams.streamId, offsetParams.eventHubNamespace,
      ehRDDPartition.eventHubNameAndPartitionID, batchTime.milliseconds, new Configuration())
    if (ehRDDPartition.fromSeq >= ehRDDPartition.untilSeq) {
      processFullyConsumedPartition(ehRDDPartition, progressWriter)
    } else {
      retrieveDataFromPartition(ehRDDPartition, progressWriter)
    }
  }
}

