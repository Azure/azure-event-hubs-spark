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

package org.apache.spark.eventhubs.common.rdd

import scala.collection.mutable.ListBuffer
import com.microsoft.azure.eventhubs.EventData
import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.eventhubs.common.NameAndPartition
import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubs.common.progress.ProgressWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }

private class EventHubRDDPartition(val sparkPartitionId: Int,
                                   val nameAndPartition: NameAndPartition,
                                   val fromOffset: Long,
                                   val fromSeq: Long,
                                   val untilSeq: Long,
                                   val offsetType: EventHubsOffsetType)
    extends Partition {
  override def index: Int = sparkPartitionId
}

private[spark] class EventHubsRDD(sc: SparkContext,
                                  ehParams: Map[String, Map[String, String]],
                                  val offsetRanges: List[OffsetRange],
                                  batchTime: Long,
                                  offsetParams: ProgressTrackerParams,
                                  receiverFactory: (Map[String, String]) => Client)
    extends RDD[EventData](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map {
      case (offsetRange, index) =>
        new EventHubRDDPartition(index,
                                 offsetRange.nameAndPartition,
                                 offsetRange.fromOffset,
                                 offsetRange.fromSeq,
                                 offsetRange.untilSeq,
                                 offsetRange.offsetType)
    }.toArray
  }

  private def wrappingReceive(nameAndPartition: NameAndPartition,
                              client: Client,
                              expectedEvents: Int,
                              untilSeqNo: Long): List[EventData] = {
    val receivedBuffer = new ListBuffer[EventData]
    while (receivedBuffer.size < expectedEvents) {
      val receivedEvents = client.receive(expectedEvents - receivedBuffer.size)
      if (receivedEvents == null) return receivedBuffer.toList // no more messages
      receivedBuffer ++= receivedEvents
      if (receivedBuffer.nonEmpty &&
          receivedBuffer.last.getSystemProperties.getSequenceNumber >= untilSeqNo) {
        // TODO: What is untilSeqNo set to on first batch when Enqueue Time is used?
        return receivedBuffer.toList
      }
    }
    receivedBuffer.toList
  }

  private def extractOffsetAndSeqToWrite(receivedEvents: List[EventData],
                                         ehReceiver: Client,
                                         ehRDDPartition: EventHubRDDPartition): (Long, Long) = {
    if (receivedEvents.nonEmpty) {
      val lastEvent = receivedEvents.last
      (lastEvent.getSystemProperties.getOffset.toLong,
       lastEvent.getSystemProperties.getSequenceNumber)
    } else {
      val partitionInfo = ehReceiver.client
        .getPartitionRuntimeInformation(ehRDDPartition.nameAndPartition.partitionId.toString)
        .get()
      (partitionInfo.getLastEnqueuedOffset.toLong, partitionInfo.getLastEnqueuedSequenceNumber)
    }
  }

  private def retrieveDataFromPartition(ehRDDPartition: EventHubRDDPartition,
                                        progressWriter: ProgressWriter): Iterator[EventData] = {
    val fromOffset = ehRDDPartition.fromOffset
    val fromSeq = ehRDDPartition.fromSeq
    val untilSeq = ehRDDPartition.untilSeq
    val expectedEvents = (untilSeq - fromSeq).toInt
    val startTime = System.currentTimeMillis()
    logInfo(
      s"${ehRDDPartition.nameAndPartition}: " +
        s"Expected Rate $expectedEvents, (fromSeq $fromSeq, untilSeq $untilSeq] at $batchTime")

    val params = ehParams(ehRDDPartition.nameAndPartition.ehName)
    val eventHubReceiver = receiverFactory(params)
    eventHubReceiver.initReceiver(ehRDDPartition.nameAndPartition.partitionId.toString,
                                  ehRDDPartition.offsetType,
                                  fromOffset.toString)
    try {
      val receivedEvents = wrappingReceive(ehRDDPartition.nameAndPartition,
                                           eventHubReceiver,
                                           expectedEvents,
                                           ehRDDPartition.untilSeq)
      logInfo(
        s"received ${receivedEvents.length} messages before Event Hubs server indicates" +
          s" there is no more messages, time cost:" +
          s" ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")
      val (offsetToWrite, seqToWrite) =
        extractOffsetAndSeqToWrite(receivedEvents, eventHubReceiver, ehRDDPartition)
      progressWriter.write(batchTime, offsetToWrite, seqToWrite)
      logInfo(
        s"write offset $offsetToWrite, sequence number $seqToWrite for EventHub" +
          s" ${ehRDDPartition.nameAndPartition} at $batchTime")
      receivedEvents.iterator
    } catch {
      case e: Exception => throw e
    } finally {
      if (eventHubReceiver != null) eventHubReceiver.close()
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[EventData] = {
    val ehRDDPartition = split.asInstanceOf[EventHubRDDPartition]
    val progressWriter = new ProgressWriter(
      offsetParams.streamId,
      offsetParams.uid,
      ehRDDPartition.nameAndPartition,
      batchTime,
      new Configuration(),
      offsetParams.checkpointDir,
      offsetParams.subDirs: _*
    )
    if (ehRDDPartition.fromSeq >= ehRDDPartition.untilSeq) {
      logInfo(s"No new data in ${ehRDDPartition.nameAndPartition} at $batchTime")
      progressWriter.write(batchTime, ehRDDPartition.fromOffset, ehRDDPartition.fromSeq)
      logInfo(s"write offset ${ehRDDPartition.fromOffset}, sequence number" +
        s" ${ehRDDPartition.fromSeq} for EventHub ${ehRDDPartition.nameAndPartition} at $batchTime")
      Iterator()
    } else {
      retrieveDataFromPartition(ehRDDPartition, progressWriter)
    }
  }
}
