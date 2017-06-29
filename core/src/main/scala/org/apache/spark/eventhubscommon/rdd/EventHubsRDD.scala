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
import org.apache.spark.eventhubscommon.client.{CachedEventHubsReceiver, EventHubsReceiver, EventHubsReceiverWrapper}
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubscommon.progress.ProgressWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.{Partition, SparkContext, TaskContext}
// scalastyle:on

private class EventHubRDDPartition(
    val sparkPartitionId: Int,
    val eventHubNameAndPartitionID: EventHubNameAndPartition,
    val fromOffset: Long,
    val fromSeq: Long,
    val untilSeq: Long,
    val offsetType: EventHubsOffsetType) extends Partition {

  override def index: Int = sparkPartitionId
}

private[spark] class EventHubsRDD(
    sc: SparkContext,
    eventHubsParamsMap: Map[String, Map[String, String]],
    val offsetRanges: List[OffsetRange],
    batchTime: Long,
    batchInterval: Long,
    offsetParams: OffsetStoreParams,
    eventHubReceiverCreator: (Map[String, String], Int, Long, EventHubsOffsetType, Int) =>
      EventHubsReceiverWrapper,
    useCachedReceiver: Boolean) extends RDD[EventData](sc, Nil) {

  import EventHubsReceiver._

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (offsetRange, index) =>
      new EventHubRDDPartition(index, offsetRange.eventHubNameAndPartition, offsetRange.fromOffset,
        offsetRange.fromSeq, offsetRange.untilSeq, offsetRange.offsetType)
    }.toArray
  }

  private def wrappingReceive[T: EventHubsReceiver](
      eventHubNameAndPartition: EventHubNameAndPartition,
      eventHubReceiverCreator: T,
      expectedEventNumber: Int): List[EventData] = {
    val receiver = implicitly[EventHubsReceiver[T]]
    val eventHubClient = eventHubReceiverCreator
    val receivedBuffer = new ListBuffer[EventData]
    val receivingTrace = new ListBuffer[Long]
    var cnt = 0
    while (receivedBuffer.size < expectedEventNumber) {
      if (cnt > expectedEventNumber * 2) {
        throw new Exception(s"$eventHubNameAndPartition cannot return data, the trace is" +
          s" ${receivingTrace.toList}")
      }
      val receivedEventsItr = receiver.receive(eventHubClient, expectedEventNumber)
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

  private def executors(): Array[ExecutorCacheTaskLocation] = {
    val bm = sparkContext.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compareExecutors)
  }

  private def compareExecutors(
      a: ExecutorCacheTaskLocation,
      b: ExecutorCacheTaskLocation): Boolean = {
    if (a.host == b.host) {
      a.executorId > b.executorId
    } else {
      a.host > b.host
    }
  }

  override def getPreferredLocations(part: Partition): Seq[String] = {
    val eventHubsPart = part.asInstanceOf[EventHubRDDPartition]
    val allExecs = executors()
    val ehNameAndPartition = eventHubsPart.eventHubNameAndPartitionID
    // execs is sorted, tp.hashCode depends only on topic and partition, so consistent index
    if (allExecs.nonEmpty) {
      val index = Math.floorMod(ehNameAndPartition.hashCode(), allExecs.length)
      val chosen = allExecs(index)
      Seq(chosen.toString)
    } else {
      Seq()
    }
  }

  private def processExaustedPartition(
      progressWriter: ProgressWriter,
      eventHubsPartition: EventHubRDDPartition):
    Iterator[EventData] = {
    logInfo(s"No new data in ${eventHubsPartition.eventHubNameAndPartitionID} at $batchTime")
    val fromOffset = eventHubsPartition.fromOffset
    progressWriter.write(batchTime, eventHubsPartition.fromOffset,
      eventHubsPartition.fromSeq)
    logInfo(s"write offset $fromOffset, sequence number" +
      s" ${eventHubsPartition.fromSeq} for EventHub" +
      s" ${eventHubsPartition.eventHubNameAndPartitionID} at $batchTime")
    Iterator()
  }

  private def createEventHubsReceiver(eventHubsPartition: EventHubRDDPartition):
      Either[EventHubsReceiverWrapper, CachedEventHubsReceiver] = {
    val eventHubParameters = eventHubsParamsMap(eventHubsPartition.eventHubNameAndPartitionID.
      eventHubName)
    val fromOffset = eventHubsPartition.fromOffset
    val maxRate = (eventHubsPartition.untilSeq - eventHubsPartition.fromSeq).toInt
    if (!useCachedReceiver) {
      Left(eventHubReceiverCreator(eventHubParameters,
        eventHubsPartition.eventHubNameAndPartitionID.partitionId, fromOffset,
        eventHubsPartition.offsetType, maxRate))
    } else {
      Right(new CachedEventHubsReceiver(eventHubParameters,
        eventHubsPartition.eventHubNameAndPartitionID.partitionId,
        fromOffset, eventHubsPartition.offsetType, maxRate, batchInterval, batchTime))
    }
  }

  private def fetchDataFromEventHubs(
      progressWriter: ProgressWriter,
      eventHubsPartition: EventHubRDDPartition): Iterator[EventData] = {
    val maxRate = (eventHubsPartition.untilSeq - eventHubsPartition.fromSeq).toInt
    val startTime = System.currentTimeMillis()
    logInfo(s"${eventHubsPartition.eventHubNameAndPartitionID}" +
      s" expected rate $maxRate, fromSeq ${eventHubsPartition.fromSeq} (exclusive) untilSeq" +
      s" ${eventHubsPartition.untilSeq} (inclusive) at $batchTime")
    val eventHubReceiver = createEventHubsReceiver(eventHubsPartition)
    val receivedEvents = eventHubReceiver match {
      case Left(uncachedReceiver) =>
        wrappingReceive(eventHubsPartition.eventHubNameAndPartitionID, uncachedReceiver,
          maxRate)
      case Right(cachedReceiver) =>
        wrappingReceive(eventHubsPartition.eventHubNameAndPartitionID, cachedReceiver,
          maxRate)
    }
    logInfo(s"received ${receivedEvents.length} messages before Event Hubs server indicates" +
      s" there is no more messages, time cost:" +
      s" ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")
    val lastEvent = receivedEvents.last
    val endOffset = lastEvent.getSystemProperties.getOffset.toLong
    val endSeq = lastEvent.getSystemProperties.getSequenceNumber
    progressWriter.write(batchTime, endOffset, endSeq)
    logInfo(s"write offset $endOffset, sequence number $endSeq for EventHub" +
      s" ${eventHubsPartition.eventHubNameAndPartitionID} at $batchTime")
    if (eventHubReceiver.isLeft) {
      eventHubReceiver.left.get.close()
    }
    receivedEvents.iterator
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[EventData] = {
    val eventHubPartition = split.asInstanceOf[EventHubRDDPartition]
    val progressWriter = new ProgressWriter(offsetParams.streamId, offsetParams.uid,
      eventHubPartition.eventHubNameAndPartitionID, batchTime, new Configuration(),
      offsetParams.checkpointDir, offsetParams.subDirs: _*)
    val fromOffset = eventHubPartition.fromOffset
    if (eventHubPartition.fromSeq >= eventHubPartition.untilSeq) {
      processExaustedPartition(progressWriter, eventHubPartition)
    } else {
      fetchDataFromEventHubs(progressWriter, eventHubPartition)
    }
  }
}

