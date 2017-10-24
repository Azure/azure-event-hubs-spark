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

import java.io.{ IOException, ObjectInputStream }

import scala.collection.mutable
import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.common.{
  NameAndPartition,
  EventHubsConnector,
  OffsetRecord,
  RateControlUtils
}
import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange, OffsetStoreParams }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ StreamingContext, Time }
import org.apache.spark.streaming.dstream.{ DStreamCheckpointData, InputDStream }
import org.apache.spark.streaming.eventhubs.checkpoint._
import org.apache.spark.streaming.scheduler.{ RateController, StreamInputInfo }
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.util.Utils

/**
 * implementation of EventHub-based direct stream
 * @param _ssc the streaming context this stream belongs to
 * @param progressDir the path of directory saving the progress file
 * @param ehParams the parameters of your eventhub instances, format:
 *                    Map[eventhubinstanceName -> Map(parameterName -> parameterValue)
 */
private[spark] class EventHubDirectDStream private[spark] (
    _ssc: StreamingContext,
    progressDir: String,
    ehParams: Map[String, Map[String, String]],
    clientFactory: (Map[String, String]) => Client)
    extends InputDStream[EventData](_ssc)
    with EventHubsConnector
    with Logging {
  // This uniquely identifies entities on the EventHubs side
  val ehNamespace: String = ehParams(ehParams.keySet.head)("eventhubs.namespace")
  for (ehName <- ehParams.keySet)
    require(
      ehParams(ehName)("eventhubs.namespace") == ehNamespace,
      "Multiple namespaces detected in ehParams. DStreams cannot be created across multiple namespaces."
    )
  override def uid: String = ehNamespace

  private[streaming] override def name: String = s"EventHubs Direct DStream [$id]"

  protected[streaming] override val checkpointData = new EventHubDirectDStreamCheckpointData(this)

  // the list of eventhubs partitions connecting with this connector
  override def namesAndPartitions: List[NameAndPartition] = {
    for (eventHubName <- ehParams.keySet;
         partitionId <- 0 until ehParams(eventHubName)("eventhubs.partition.count").toInt)
      yield NameAndPartition(eventHubName, partitionId)
  }.toList

  private var latestCheckpointTime: Time = _
  private var initialized = false

  DirectDStreamProgressTracker.registeredConnectors += this

  override protected[streaming] val rateController: Option[RateController] = {
    // TODO: performance evaluation of rate controller
    if (RateController.isBackPressureEnabled(ssc.sparkContext.conf)) {
      logInfo("back pressure is not currently supported")
      None
      /*Some(
        new EventHubDirectDStreamRateController(
          id,
          RateEstimator.create(ssc.sparkContext.conf, graph.batchDuration)))*/
    } else {
      None
    }
  }

  private def progressTracker =
    DirectDStreamProgressTracker.getInstance.asInstanceOf[DirectDStreamProgressTracker]

  @transient private var _clients: mutable.HashMap[String, Client] = _
  private[eventhubs] def ehClients = {
    if (_clients == null) {
      _clients = new mutable.HashMap[String, Client].empty
      for (name <- ehParams.keys)
        _clients += name -> clientFactory(ehParams(name))
    }
    _clients
  }

  private[eventhubs] def ehClients_=(client: mutable.HashMap[String, Client]) = {
    _clients = client
    this
  }

  private[eventhubs] var currentOffsetsAndSeqNums = OffsetRecord(-1L, {
    namesAndPartitions.map { ehNameAndSpace =>
      (ehNameAndSpace, (-1L, -1L))
    }.toMap
  })
  private[eventhubs] var fetchedHighestOffsetsAndSeqNums: OffsetRecord = _

  override def start(): Unit = {
    val concurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
    require(
      concurrentJobs == 1,
      "due to the limitation from eventhub, we do not allow to have multiple concurrent spark jobs")
    DirectDStreamProgressTracker.initInstance(progressDir,
                                              context.sparkContext.appName,
                                              context.sparkContext.hadoopConfiguration)
    ProgressTrackingListener.initInstance(ssc, progressDir)
  }

  override def stop(): Unit = {
    logInfo("stop: stopping EventHubDirectDStream")
  }

  /**
   * EventHub uses *Number Of Messages* for rate control, but uses *offset* to setup of the start
   * point of the receivers. As a result, we need to translate the sequence number to offset to
   * start receiver in the next batch, which is a functionality not provided by EventHub. We use
   * checkpoint file to communicate this information between executors and driver.
   *
   * In this function, we either read startpoint from checkpoint file or start processing from
   * the very beginning of the streams
   *
   * @return EventHubName-Partition -> (offset, seq)
   */
  private def fetchStartOffsetForEachPartition(validTime: Time, fallBack: Boolean): OffsetRecord = {
    val offsetRecord = progressTracker.read(
      ehNamespace,
      validTime.milliseconds - ssc.graph.batchDuration.milliseconds,
      fallBack)
    require(offsetRecord.offsets.nonEmpty, "progress file cannot be empty")
    if (offsetRecord.timestamp != -1) {
      OffsetRecord(math.max(ssc.graph.startTime.milliseconds, offsetRecord.timestamp),
                   offsetRecord.offsets)
    } else {
      // query start startSeqs
      val startSeqs = new mutable.HashMap[NameAndPartition, Long].empty
      for (nameAndPartition <- namesAndPartitions) {
        val name = nameAndPartition.ehName
        val seqNo = ehClients(name).beginSeqNo(nameAndPartition)
        require(seqNo.isDefined, s"Failed to get starting sequence number for $nameAndPartition")

        startSeqs += nameAndPartition -> seqNo.get
      }

      OffsetRecord(
        math.max(ssc.graph.startTime.milliseconds, offsetRecord.timestamp),
        offsetRecord.offsets.map {
          case (ehNameAndPartition, (offset, _)) =>
            (ehNameAndPartition, (offset, startSeqs(ehNameAndPartition)))
        }
      )
    }
  }

  private def reportInputInto(validTime: Time,
                              offsetRanges: List[OffsetRange],
                              inputSize: Int): Unit = {
    require(inputSize >= 0, s"invalid inputSize ($inputSize) with offsetRanges: $offsetRanges")
    val description = offsetRanges
      .map { offsetRange =>
        s"eventhub: ${offsetRange.nameAndPartition}\t" +
          s"starting offsets: ${offsetRange.fromOffset}" +
          s"sequenceNumbers: ${offsetRange.fromSeq} to ${offsetRange.untilSeq}"
      }
      .mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata =
      Map("offsets" -> offsetRanges, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, inputSize, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
  }

  private def validatePartitions(validTime: Time,
                                 calculatedPartitions: List[NameAndPartition]): Unit = {
    if (currentOffsetsAndSeqNums != null) {
      val currentPartitions = currentOffsetsAndSeqNums.offsets.keys.toList
      val diff = currentPartitions.diff(calculatedPartitions)
      if (diff.nonEmpty) {
        logError(s"detected lost partitions $diff")
        throw new RuntimeException(s"some partitions are lost before $validTime")
      }
    }
  }

  private def clamp(
      highestEndpoints: Map[NameAndPartition, (Long, Long)]): Map[NameAndPartition, Long] = {
    if (rateController.isEmpty) {
      RateControlUtils.clamp(currentOffsetsAndSeqNums.offsets,
                             fetchedHighestOffsetsAndSeqNums.offsets,
                             ehParams)
    } else {
      val estimateRateLimit = rateController.map(_.getLatestRate().toInt)
      estimateRateLimit.filter(_ > 0) match {
        case None =>
          highestEndpoints.map {
            case (ehNameAndPartition, _) =>
              (ehNameAndPartition, currentOffsetsAndSeqNums.offsets(ehNameAndPartition)._2)
          }
        case Some(allowedRate) =>
          val lagPerPartition = highestEndpoints.map {
            case (eventHubNameAndPartition, (_, latestSeq)) =>
              eventHubNameAndPartition ->
                math.max(latestSeq - currentOffsetsAndSeqNums.offsets(eventHubNameAndPartition)._2,
                         0)
          }
          val totalLag = lagPerPartition.values.sum
          lagPerPartition.map {
            case (eventHubNameAndPartition, lag) =>
              val backpressureRate = math.round(lag / totalLag.toFloat * allowedRate)
              eventHubNameAndPartition ->
                (backpressureRate + currentOffsetsAndSeqNums.offsets(eventHubNameAndPartition)._2)
          }
      }
    }
  }

  // we should only care about the passing offset types when we start for the first time of the
  // application; this "first time" shall not include the restart from checkpoint
  private def shouldCareEnqueueTimeOrOffset = !initialized && !ssc.isCheckpointPresent

  private def composeOffsetRange(
      startOffsetInNextBatch: OffsetRecord,
      highestOffsets: Map[NameAndPartition, (Long, Long)]): List[OffsetRange] = {
    val clampedSeqIDs = clamp(highestOffsets)
    // to handle filter.enqueueTime and filter.offset
    val filteringOffsetAndType = {
      if (shouldCareEnqueueTimeOrOffset) {
        // first check if the parameters are valid
        RateControlUtils.validateFilteringParams(ehClients.toMap, ehParams, namesAndPartitions)
        RateControlUtils.composeFromOffsetWithFilteringParams(ehParams,
                                                              startOffsetInNextBatch.offsets)
      } else {
        Map[NameAndPartition, (EventHubsOffsetType, Long)]()
      }
    }
    highestOffsets.map {
      case (eventHubNameAndPartition, (_, endSeqNum)) =>
        val (offsetType, offset) =
          RateControlUtils.calculateStartOffset(eventHubNameAndPartition,
                                                filteringOffsetAndType,
                                                startOffsetInNextBatch.offsets)
        OffsetRange(
          eventHubNameAndPartition,
          fromOffset = offset,
          fromSeq = startOffsetInNextBatch.offsets(eventHubNameAndPartition)._2,
          untilSeq = math.min(clampedSeqIDs(eventHubNameAndPartition), endSeqNum),
          offsetType = offsetType
        )
    }.toList
  }

  private def proceedWithNonEmptyRDD(
      validTime: Time,
      startOffsetInNextBatch: OffsetRecord,
      highestOffsetOfAllPartitions: Map[NameAndPartition, (Long, Long)]): Option[EventHubsRDD] = {
    // normal processing
    validatePartitions(validTime, startOffsetInNextBatch.offsets.keys.toList)
    currentOffsetsAndSeqNums = startOffsetInNextBatch
    logInfo(s"starting batch at $validTime with $startOffsetInNextBatch")
    val offsetRanges = composeOffsetRange(startOffsetInNextBatch, highestOffsetOfAllPartitions)
    val eventHubRDD = new EventHubsRDD(
      context.sparkContext,
      ehParams,
      offsetRanges,
      validTime.milliseconds,
      OffsetStoreParams(progressDir,
                        streamId,
                        uid = ehNamespace,
                        subDirs = ssc.sparkContext.appName),
      clientFactory
    )
    reportInputInto(validTime,
                    offsetRanges,
                    offsetRanges.map(ofr => ofr.untilSeq - ofr.fromSeq).sum.toInt)
    Some(eventHubRDD)
  }

  override private[streaming] def clearCheckpointData(time: Time): Unit = {
    super.clearCheckpointData(time)
    EventHubDirectDStream.cleanupLock.synchronized {
      if (EventHubDirectDStream.lastCleanupTime < time.milliseconds) {
        logInfo(s"clean up progress file which is earlier than ${time.milliseconds}")
        progressTracker.cleanProgressFile(time.milliseconds)
        EventHubDirectDStream.lastCleanupTime = time.milliseconds
      }
    }
  }

  private[spark] def composeHighestOffset(validTime: Time) = {
    RateControlUtils.fetchLatestOffset(
      ehClients.toMap,
      if (fetchedHighestOffsetsAndSeqNums == null) {
        currentOffsetsAndSeqNums.offsets
      } else {
        fetchedHighestOffsetsAndSeqNums.offsets
      }
    ) match {
      case Some(highestOffsets) =>
        fetchedHighestOffsetsAndSeqNums = OffsetRecord(validTime.milliseconds, highestOffsets)
        Some(fetchedHighestOffsetsAndSeqNums.offsets)
      case _ =>
        logWarning(s"failed to fetch highest offset at $validTime")
        None
    }
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    if (!initialized) {
      ProgressTrackingListener.initInstance(ssc, progressDir)
    }
    require(progressTracker != null, "ProgressTracker hasn't been initialized")
    var startPointRecord = fetchStartOffsetForEachPartition(validTime, !initialized)
    while (startPointRecord.timestamp < validTime.milliseconds -
             ssc.graph.batchDuration.milliseconds) {
      logInfo(
        s"wait for ProgressTrackingListener to commit offsets at Batch" +
          s" ${validTime.milliseconds}")
      graph.wait()
      logInfo(s"wake up at Batch ${validTime.milliseconds} at DStream $id")
      startPointRecord = fetchStartOffsetForEachPartition(validTime, !initialized)
    }
    // we need to update highest offset after we skip or get out from the while loop, because
    // 1) when the user pass in filtering params they may have received events whose seq number
    // is higher than the saved one -> leads to an exception;
    // 2) when the last batch was delayed, we should catch up by detecting the latest highest
    // offset
    val highestOffsetOption = composeHighestOffset(validTime)
    require(highestOffsetOption.isDefined,
            "We cannot get starting highest offset of partitions," +
              " EventHubs endpoint is not available")
    logInfo(s"highestOffsetOfAllPartitions at $validTime: ${highestOffsetOption.get}")
    logInfo(
      s"$validTime currentOffsetTimestamp: ${currentOffsetsAndSeqNums.timestamp}\t" +
        s" startPointRecordTimestamp: ${startPointRecord.timestamp}")
    val rdd = proceedWithNonEmptyRDD(validTime, startPointRecord, highestOffsetOption.get)
    initialized = true
    rdd
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    DirectDStreamProgressTracker.registeredConnectors += this
    initialized = false
  }

  private[eventhubs] class EventHubDirectDStreamCheckpointData(
      eventHubDirectDStream: EventHubDirectDStream)
      extends DStreamCheckpointData(this) {

    def batchForTime
      : mutable.HashMap[Time, Array[(NameAndPartition, Long, Long, Long, EventHubsOffsetType)]] = {
      data.asInstanceOf[
        mutable.HashMap[Time, Array[(NameAndPartition, Long, Long, Long, EventHubsOffsetType)]]]
    }

    override def update(time: Time): Unit = {
      if (latestCheckpointTime == null || time > latestCheckpointTime) {
        latestCheckpointTime = time
      }
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val offsetRangeOfRDD = kv._2.asInstanceOf[EventHubsRDD].offsetRanges.map(_.toTuple).toArray
        logInfo(s"update RDD ${offsetRangeOfRDD.mkString("[", ", ", "]")} at ${kv._1}")
        batchForTime += kv._1 -> offsetRangeOfRDD
      }
    }

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {
      // we have to initialize here, otherwise there is a race condition when recovering from spark
      // checkpoint
      logInfo("initialized ProgressTracker")
      val appName = context.sparkContext.appName
      DirectDStreamProgressTracker.initInstance(progressDir,
                                                appName,
                                                context.sparkContext.hadoopConfiguration)
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, b) =>
          logInfo(s"Restoring EventHubRDD for time $t ${b.mkString("[", ", ", "]")}")
          generatedRDDs += t -> new EventHubsRDD(
            context.sparkContext,
            ehParams,
            b.map {
              case (ehNameAndPar, fromOffset, fromSeq, untilSeq, offsetType) =>
                OffsetRange(ehNameAndPar, fromOffset, fromSeq, untilSeq, offsetType)
            }.toList,
            t.milliseconds,
            OffsetStoreParams(progressDir, streamId, uid = ehNamespace, subDirs = appName),
            clientFactory
          )
      }
    }
  }

  private[eventhubs] class EventHubDirectDStreamRateController(id: Int, estimator: RateEstimator)
      extends RateController(id, estimator) {
    override protected def publish(rate: Long): Unit = {
      // publish nothing as there is no receiver
    }
  }

  // the id of the stream which is mapped from eventhubs instance
  override val streamId: Int = this.id
}

private[eventhubs] object EventHubDirectDStream {
  val cleanupLock = new Object
  private[eventhubs] var lastCleanupTime = -1L
}
