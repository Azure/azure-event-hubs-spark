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

import java.io.{IOException, ObjectInputStream}

import scala.collection.mutable

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.eventhubscommon._
import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper, RestfulEventHubClient}
import org.apache.spark.eventhubscommon.rdd.{EventHubsRDD, OffsetRange, OffsetStoreParams}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.eventhubs.checkpoint._
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.util.Utils

/**
 * implementation of EventHub-based direct stream
 * @param _ssc the streaming context this stream belongs to
 * @param eventHubNameSpace the namespace of evenhub instances
 * @param progressDir the checkpoint directory path (we only support HDFS-based checkpoint
 *                      storage for now, so you have to prefix your path with hdfs://clustername/
 * @param eventhubsParams the parameters of your eventhub instances, format:
 *                    Map[eventhubinstanceName -> Map(parameterName -> parameterValue)
 */
private[eventhubs] class EventHubDirectDStream private[eventhubs] (
    _ssc: StreamingContext,
    private[eventhubs] val eventHubNameSpace: String,
    progressDir: String,
    eventhubsParams: Map[String, Map[String, String]],
    eventhubReceiverCreator: (Map[String, String], Int, Long, Int) => EventHubsClientWrapper =
                                              EventHubsClientWrapper.getEventHubReceiver,
    eventhubClientCreator: (String, Map[String, Map[String, String]]) => EventHubClient =
                                              RestfulEventHubClient.getInstance)
  extends InputDStream[EventData](_ssc) with EventHubsConnector with Logging {

  private[streaming] override def name: String = s"EventHub direct stream [$id]"

  private var latestCheckpointTime: Time = _

  private var initialized = false

  DirectDStreamProgressTracker.registeredConnectors += this

  protected[streaming] override val checkpointData = new EventHubDirectDStreamCheckpointData(this)

  private[eventhubs] val eventhubNameAndPartitions = {
    for (eventHubName <- eventhubsParams.keySet;
         partitionId <- 0 until eventhubsParams(eventHubName)(
      "eventhubs.partition.count").toInt) yield EventHubNameAndPartition(eventHubName, partitionId)
  }

  // uniquely identify the entities in eventhubs side, it can be the namespace or the name of a
  override def uid: String = eventHubNameSpace

  // the list of eventhubs partitions connecting with this connector
  override def connectedInstances: List[EventHubNameAndPartition] = eventhubNameAndPartitions.toList

  override protected[streaming] val rateController: Option[RateController] = {
    None
    // TODO: performance evaluation of rate controller
    /*
    if (RateController.isBackPressureEnabled(ssc.sparkContext.conf)) {
      Some(new EventHubDirectDStreamRateController(id, RateEstimator.create(ssc.sparkContext.conf,
        graph.batchDuration)))
    } else {
      None
    }
    */
  }

  private var _eventHubClient: EventHubClient = _

  private def progressTracker = DirectDStreamProgressTracker.getInstance.
    asInstanceOf[DirectDStreamProgressTracker]

  private[eventhubs] def setEventHubClient(eventHubClient: EventHubClient):
      EventHubDirectDStream = {
    _eventHubClient = eventHubClient
    this
  }

  private[eventhubs] def eventHubClient = {
    if (_eventHubClient == null) {
      _eventHubClient = eventhubClientCreator(eventHubNameSpace, eventhubsParams)
    }
    _eventHubClient
  }

  private[eventhubs] var currentOffsetsAndSeqNums = OffsetRecord(Time(-1),
    {eventhubNameAndPartitions.map{ehNameAndSpace => (ehNameAndSpace, (-1L, -1L))}.toMap})
  private[eventhubs] var fetchedHighestOffsetsAndSeqNums: OffsetRecord = _

  override def start(): Unit = {
    val concurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
    require(concurrentJobs == 1,
      "due to the limitation from eventhub, we do not allow to have multiple concurrent spark jobs")
    DirectDStreamProgressTracker.initInstance(progressDir,
      context.sparkContext.appName, context.sparkContext.hadoopConfiguration)
    ProgressTrackingListener.initInstance(ssc, progressDir)
  }

  override def stop(): Unit = {
    eventHubClient.close()
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
    val offsetRecord = progressTracker.read(eventHubNameSpace,
      validTime.milliseconds - ssc.graph.batchDuration.milliseconds, fallBack)
    require(offsetRecord.offsets.nonEmpty, "progress file cannot be empty")
    OffsetRecord(Time(math.max(ssc.graph.startTime.milliseconds,
      offsetRecord.timestamp.milliseconds)), offsetRecord.offsets)
  }

  private def reportInputInto(validTime: Time,
                              offsetRanges: List[OffsetRange], inputSize: Int): Unit = {
    require(inputSize >= 0, s"invalid inputSize ($inputSize) with offsetRanges: $offsetRanges")
    val description = offsetRanges.map { offsetRange =>
      s"eventhub: ${offsetRange.eventHubNameAndPartition}\t" +
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

  private def validatePartitions(
      validTime: Time,
      calculatedPartitions: List[EventHubNameAndPartition]): Unit = {
    if (currentOffsetsAndSeqNums != null) {
      val currentPartitions = currentOffsetsAndSeqNums.offsets.keys.toList
      val diff = currentPartitions.diff(calculatedPartitions)
      if (diff.nonEmpty) {
        logError(s"detected lost partitions $diff")
        throw new RuntimeException(s"some partitions are lost before $validTime")
      }
    }
  }

  private def clamp(highestEndpoints: Map[EventHubNameAndPartition, (Long, Long)]):
      Map[EventHubNameAndPartition, Long] = {
    if (rateController.isEmpty) {
      RateControlUtils.clamp(currentOffsetsAndSeqNums.offsets,
        fetchedHighestOffsetsAndSeqNums.offsets, eventhubsParams)
    } else {
      val estimateRateLimit = rateController.map(_.getLatestRate().toInt)
      estimateRateLimit.filter(_ > 0) match {
        case None =>
          highestEndpoints.map{case (ehNameAndPartition, _) =>
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

  private def proceedWithNonEmptyRDD(
      validTime: Time,
      startOffsetInNextBatch: OffsetRecord,
      highestOffsetOfAllPartitions: Map[EventHubNameAndPartition, (Long, Long)]):
    Option[EventHubsRDD] = {
    // normal processing
    validatePartitions(validTime, startOffsetInNextBatch.offsets.keys.toList)
    currentOffsetsAndSeqNums = startOffsetInNextBatch
    val clampedSeqIDs = clamp(highestOffsetOfAllPartitions)
    logInfo(s"starting batch at $validTime with $startOffsetInNextBatch")
    val offsetRanges = highestOffsetOfAllPartitions.map {
      case (eventHubNameAndPartition, (_, endSeqNum)) =>
        OffsetRange(eventHubNameAndPartition,
          fromOffset = startOffsetInNextBatch.offsets(eventHubNameAndPartition)._1,
          fromSeq = startOffsetInNextBatch.offsets(eventHubNameAndPartition)._2,
          untilSeq = math.min(clampedSeqIDs(eventHubNameAndPartition), endSeqNum))
    }.toList
    val eventHubRDD = new EventHubsRDD(
      context.sparkContext,
      eventhubsParams,
      offsetRanges,
      validTime.milliseconds,
      OffsetStoreParams(progressDir, streamId, uid = eventHubNameSpace,
        subDirs = ssc.sparkContext.appName),
      eventhubReceiverCreator)
    reportInputInto(validTime, offsetRanges,
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

  /**
   * when we have reached the end of the message queue in the remote end or we haven't get any
   * idea about the highest offset, we shall fail the app when rest endpoint is not responsive, and
   * to prevent we die too much, we shall retry with 2-power interval in this case
   */
  private def failAppIfRestEndpointFail = fetchedHighestOffsetsAndSeqNums == null ||
    currentOffsetsAndSeqNums.offsets.equals(fetchedHighestOffsetsAndSeqNums.offsets)

  private[spark] def composeHighestOffset(validTime: Time, retryIfFail: Boolean) = {
    RateControlUtils.fetchLatestOffset(
      eventHubClient,
      retryIfFail,
      if (fetchedHighestOffsetsAndSeqNums == null) {
        null
      } else {
        fetchedHighestOffsetsAndSeqNums.offsets
      },
      currentOffsetsAndSeqNums.offsets) match {
      case Some(highestOffsets) =>
        fetchedHighestOffsetsAndSeqNums = OffsetRecord(validTime, highestOffsets)
        Some(fetchedHighestOffsetsAndSeqNums.offsets)
      case _ =>
        logWarning(s"failed to fetch highest offset at $validTime")
        if (retryIfFail) {
          None
        } else {
          Some(fetchedHighestOffsetsAndSeqNums.offsets)
        }
    }
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    if (!initialized) {
      ProgressTrackingListener.initInstance(ssc, progressDir)
    }
    require(progressTracker != null, "ProgressTracker hasn't been initialized")
    val highestOffsetOption = composeHighestOffset(validTime, failAppIfRestEndpointFail)
    logInfo(s"highestOffsetOfAllPartitions at $validTime: $highestOffsetOption")
    if (highestOffsetOption.isEmpty) {
      val errorMsg = s"EventHub $eventHubNameSpace Rest Endpoint is not responsive, will" +
        s" stop the application"
      logError(errorMsg)
      throw new IllegalStateException(errorMsg)
    } else {
      var startPointRecord = fetchStartOffsetForEachPartition(validTime, !initialized)
      while (startPointRecord.timestamp < validTime - ssc.graph.batchDuration) {
        logInfo(s"wait for ProgressTrackingListener to commit offsets at Batch" +
          s" ${validTime.milliseconds}")
        graph.wait()
        logInfo(s"wake up at Batch ${validTime.milliseconds} at DStream $id")
        startPointRecord = fetchStartOffsetForEachPartition(validTime, !initialized)
      }
      logInfo(s"$validTime currentOffsetTimestamp: ${currentOffsetsAndSeqNums.timestamp}\t" +
        s" startPointRecordTimestamp: ${startPointRecord.timestamp}")
      initialized = true
      proceedWithNonEmptyRDD(validTime, startPointRecord, highestOffsetOption.get)
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    DirectDStreamProgressTracker.registeredConnectors += this
    initialized = false
  }

  private[eventhubs] class EventHubDirectDStreamCheckpointData(
      eventHubDirectDStream: EventHubDirectDStream) extends DStreamCheckpointData(this) {

    def batchForTime: mutable.HashMap[Time, Array[(EventHubNameAndPartition, Long, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[(EventHubNameAndPartition, Long, Long, Long)]]]
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

    override def cleanup(time: Time): Unit = { }

    override def restore(): Unit = {
      // we have to initialize here, otherwise there is a race condition when recovering from spark
      // checkpoint
      logInfo("initialized ProgressTracker")
      val appName = context.sparkContext.appName
      DirectDStreamProgressTracker.initInstance(progressDir, appName,
        context.sparkContext.hadoopConfiguration)
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring EventHubRDD for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new EventHubsRDD(
          context.sparkContext,
          eventhubsParams,
          b.map {case (ehNameAndPar, fromOffset, fromSeq, untilSeq) =>
            OffsetRange(ehNameAndPar, fromOffset, fromSeq, untilSeq)}.toList,
          t.milliseconds,
          OffsetStoreParams(progressDir, streamId, uid = eventHubNameSpace,
            subDirs = appName),
          eventhubReceiverCreator)
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
  var lastCleanupTime = -1L
}
