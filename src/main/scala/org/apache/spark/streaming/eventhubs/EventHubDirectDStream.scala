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

import scala.collection.mutable

import com.microsoft.azure.eventhubs.{EventData, PartitionReceiver}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.eventhubs.checkpoint._
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.Utils

class EventHubDirectDStream(
    _ssc: StreamingContext,
    eventHubNameSpace: String,
    checkpointDir: String,
    eventhubsParams: Map[String, Map[String, String]])
  extends InputDStream[EventData](_ssc) with Logging {

  private[streaming] override def name: String = s"EventHub direct stream [$id]"

  protected[streaming] override val checkpointData = new EventHubDirectDStreamCheckpointData(this)

  @transient private[eventhubs] var _eventHubClient: EventHubClient = _

  @transient private[eventhubs] var _offsetStore: OffsetStoreNew = _

  private[eventhubs] def eventHubClient = {
    if (_eventHubClient == null) {
      _eventHubClient = new RestfulEventHubClient(eventHubNameSpace,
        numPartitionsEventHubs = {
          eventhubsParams.map { case (eventhubName, params) => (eventhubName,
            params("eventhubs.partition.count").toInt)
          }
        },
        consumerGroups = {
          eventhubsParams.map { case (eventhubName, params) => (eventhubName,
            params("eventhubs.consumergroup"))
          }
        },
        policyKeys = eventhubsParams.map { case (eventhubName, params) => (eventhubName,
          (params("eventhubs.policyname"), params("eventhubs.policykey")))
        },
        threadNum = 15)
    }
    _eventHubClient
  }

  private[eventhubs] def setOffsetStore(offsetStore: OffsetStoreNew): Unit = {
    _offsetStore = offsetStore
  }

  // Only for test
  private[eventhubs] def setEventHubClient(eventHubClient: EventHubClient): Unit = {
    _eventHubClient = eventHubClient
  }

  private[eventhubs] def offsetStore = {
    if (_offsetStore == null) {
      _offsetStore = OffsetStoreNew.newInstance(
        checkpointDir,
        ssc.sparkContext.appName,
        this.id,
        eventHubNameSpace,
        ssc.sparkContext.hadoopConfiguration)
    }
    _offsetStore
  }

  // from eventHubName to offset
  private[eventhubs] var currentOffsetsAndSeqNums: Map[EventHubNameAndPartition, (Long, Long)] =
    Map[EventHubNameAndPartition, (Long, Long)]()

  override def start(): Unit = {
    val concurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
    require(concurrentJobs == 1,
      "due to the limitation from eventhub, we do not allow to have multiple concurrent spark jobs")
    ssc.addStreamingListener(new CheckpointListener)
  }

  override def stop(): Unit = {
    // step 1: stop driver-side consumer
    eventHubClient.close()
    // step 2: ??? what and how we shall stop
  }

  private def clamp(latestOffsets: Option[Map[EventHubNameAndPartition, (Long, Long)]],
                    validTime: Time): Map[EventHubNameAndPartition, (Long, Long)] = {
    // TODO: rate control
    if (latestOffsets.isDefined) {
      latestOffsets.get
    } else {
      logWarning(s"cannot get latest offset at time $validTime")
      Map()
    }
  }

  private def calculateStartOffsetForEachPartition(validTime: Time):
    Map[EventHubNameAndPartition, (Long, Long)] = {
    val checkpoints = offsetStore.read()
    if (checkpoints.nonEmpty) {
      logInfo(s"get checkpoint at $validTime: $checkpoints")
      checkpoints
    } else {
      // start from the beginning of each eventHubPartition
      {
        for (eventHubName <- eventhubsParams.keys;
             p <- 0 until eventhubsParams(eventHubName)("eventhubs.partition.count").toInt)
          yield (EventHubNameAndPartition(eventHubName, p),
            (PartitionReceiver.START_OF_STREAM.toLong, 0L))
      }.toMap
    }
  }

  private def canStartNextBatch: Boolean = {
    offsetStore match {
      case dfsOffsetStore: DfsBasedOffsetStore2 =>
        val fs = dfsOffsetStore.checkpointTempDirPath.getFileSystem(
          ssc.sparkContext.hadoopConfiguration)
        if (fs.exists(dfsOffsetStore.checkpointTempDirPath) &&
          fs.listStatus(dfsOffsetStore.checkpointTempDirPath).length > 0) {
          logInfo(s"checkpoint hasn't been cleaned up" +
            s" ${fs.listFiles(dfsOffsetStore.checkpointTempDirPath, false).next().toString}")
          false
        } else {
          true
        }
      case _ =>
        true
    }
  }

  private def reportInputInto(validTime: Time,
                              offsetRanges: List[OffsetRange], inputSize: Int): Unit = {
    // step 3: report the metadata to InputInfoTracker
    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val description = offsetRanges.map { offsetRange =>
      s"topic: ${offsetRange.eventHubNameAndPartition}\t" +
        s"starting offsets: ${offsetRange.fromOffset}" +
        s"sequenceNumbers: ${offsetRange.fromSeq} to ${offsetRange.untilSeq}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, inputSize, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
  }

  private def validateNewlyCalculatedPartitions(
      validTime: Time,
      calculatedPartitions: List[EventHubNameAndPartition]): Unit = {
    if (currentOffsetsAndSeqNums.nonEmpty) {
      val currentPartitions = currentOffsetsAndSeqNums.keys.toList
      val diff = currentPartitions.diff(calculatedPartitions)
      if (diff.nonEmpty) {
        logError(s"detected lost partitions $diff")
        throw new RuntimeException(s"some partitions are lost before $validTime")
      }
    }
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    // step 1, detect the highest offset of each eventhub instance
    // we need to create the driver-side & restful API based consumer in step 1
    val newOffsetsAndSeqNums = calculateStartOffsetForEachPartition(validTime)
    val sameCheckpoint = newOffsetsAndSeqNums.equals(currentOffsetsAndSeqNums)
    if (sameCheckpoint || !canStartNextBatch) {
      reportInputInto(validTime, List(), 0)
      Some(ssc.sparkContext.emptyRDD[EventData])
    } else {
      val endPoints = clamp(eventHubClient.endPointOfPartition(), validTime)
      if (endPoints.nonEmpty) {
        validateNewlyCalculatedPartitions(validTime, newOffsetsAndSeqNums.keys.toList)
        currentOffsetsAndSeqNums = newOffsetsAndSeqNums
        logInfo(s"starting batch at $validTime with $currentOffsetsAndSeqNums")
        val offsetRanges = endPoints.map {
          case (eventHubNameAndPartition, (endOffset, endSeqNum)) =>
            OffsetRange(eventHubNameAndPartition,
              fromOffset = currentOffsetsAndSeqNums(eventHubNameAndPartition)._1,
              fromSeq = currentOffsetsAndSeqNums(eventHubNameAndPartition)._2,
              untilSeq =
                math.min(currentOffsetsAndSeqNums(eventHubNameAndPartition)._2 + 999, endSeqNum))
        }.toList
        val eventHubRDD = new EventHubRDD(
          this.context.sparkContext,
          eventhubsParams,
          offsetRanges,
          validTime,
          OffsetStoreParams(checkpointDir, ssc.sparkContext.appName, this.id, eventHubNameSpace))
        reportInputInto(validTime, offsetRanges,
          offsetRanges.map(ofr => ofr.untilSeq - ofr.fromSeq + 1).sum.toInt)
        Some(eventHubRDD)
      } else {
        reportInputInto(validTime, List(), 0)
        Some(ssc.sparkContext.emptyRDD[EventData])
      }
    }
  }

  private[eventhubs]
  class EventHubDirectDStreamCheckpointData(dstream: EventHubDirectDStream)
    extends DStreamCheckpointData(this) {

    def batchForTime: mutable.HashMap[Time,
      Array[(EventHubNameAndPartition, Long, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time,
        Array[(EventHubNameAndPartition, Long, Long, Long)]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[EventHubRDD].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time): Unit = { }

    override def restore(): Unit = {
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring EventHub for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new EventHubRDD(
          context.sparkContext,
          eventhubsParams,
          b.map {case (ehNameAndPar, fromOffset, fromSeq, untilSeq) =>
            OffsetRange(ehNameAndPar, fromOffset, fromSeq, untilSeq)}.toList,
          t,
          OffsetStoreParams(checkpointDir, ssc.sparkContext.appName, id, eventHubNameSpace)
        )
      }
    }
  }
}
