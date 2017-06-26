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
import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.{EventData, PartitionReceiver}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.eventhubs.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.streaming.eventhubs.checkpoint._
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.util.Utils

/**
 * implementation of EventHub-based direct stream
 * @param _ssc the streaming context this stream belongs to
 * @param eventHubNameSpace the namespace of evenhub instances
 * @param progressDir path of directory saving progress files
 * @param eventhubsParams the parameters of your eventhub instances, format:
 *                    Map[eventhubinstanceName -> Map(parameterName -> parameterValue)
 */
private[eventhubs] class EventHubDirectDStream private[eventhubs] (
    _ssc: StreamingContext,
    private[eventhubs] val eventHubNameSpace: String,
    progressDir: String,
    eventhubsParams: Map[String, Map[String, String]],
    eventhubReceiverCreator: (Map[String, String], Int, Long, EventHubsOffsetType, Int) =>
      EventHubsClientWrapper = EventHubsClientWrapper.getEventHubReceiver,
    eventhubClientCreator: (String, Map[String, Map[String, String]]) =>
      EventHubClient = AMQPEventHubsClient.getInstance)
  extends InputDStream[EventData](_ssc) with Logging {

  private[streaming] override def name: String = s"EventHub direct stream [$id]"

  private var latestCheckpointTime: Time = _

  private var initialized = false

  ProgressTracker.eventHubDirectDStreams += this

  protected[streaming] override val checkpointData = new EventHubDirectDStreamCheckpointData(this)

  private[eventhubs] val eventhubNameAndPartitions = {
    for (eventHubName <- eventhubsParams.keySet;
         partitionId <- 0 until eventhubsParams(eventHubName)(
      "eventhubs.partition.count").toInt) yield EventHubNameAndPartition(eventHubName, partitionId)
  }

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

  private def maxRateLimitPerPartition(eventHubName: String): Int = {
    val x = eventhubsParams(eventHubName).getOrElse("eventhubs.maxRate", "10000").toInt
    require(x > 0, s"eventhubs.maxRate has to be larger than zero, violated by $eventHubName ($x)")
    x
  }

  @transient private var _eventHubClient: EventHubClient = _

  private def progressTracker = ProgressTracker.getInstance

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
    ProgressTracker.initInstance(progressDir,
      context.sparkContext.appName, context.sparkContext.hadoopConfiguration)
    ProgressTrackingListener.initInstance(ssc, progressDir)
  }

  override def stop(): Unit = {
    eventHubClient.close()
  }

  private def collectPartitionsNeedingLargerProcessingRange(): List[EventHubNameAndPartition] = {
    val partitionList = new ListBuffer[EventHubNameAndPartition]
    if (fetchedHighestOffsetsAndSeqNums != null) {
      for ((ehNameAndPartition, (offset, seqId)) <- fetchedHighestOffsetsAndSeqNums.offsets) {
        if (currentOffsetsAndSeqNums.offsets(ehNameAndPartition)._2 >=
          fetchedHighestOffsetsAndSeqNums.offsets(ehNameAndPartition)._2) {
          partitionList += ehNameAndPartition
        }
      }
    } else {
      partitionList ++= eventhubNameAndPartitions
    }
    partitionList.toList
  }

  private def fetchLatestOffset(validTime: Time, retryIfFail: Boolean):
      Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    // check if there is any eventhubs partition which potentially has newly arrived message (
    // the fetched highest message id is within the next batch's processing engine)
    val demandingEhNameAndPartitions = collectPartitionsNeedingLargerProcessingRange()
    val r = eventHubClient.endPointOfPartition(retryIfFail, demandingEhNameAndPartitions)
    if (r.isDefined) {
      // merge results
      val mergedOffsets = if (fetchedHighestOffsetsAndSeqNums != null) {
        fetchedHighestOffsetsAndSeqNums.offsets ++ r.get
      } else {
        r.get
      }
      fetchedHighestOffsetsAndSeqNums = OffsetRecord(validTime, mergedOffsets)
      Some(fetchedHighestOffsetsAndSeqNums.offsets)
    } else {
      r
    }
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
    val offsetRecord = progressTracker.read(eventHubNameSpace, validTime.milliseconds,
      ssc.graph.batchDuration.milliseconds, fallBack)
    require(offsetRecord.offsets.nonEmpty, "progress file cannot be empty")
    if (offsetRecord.timestamp.milliseconds != -1) {
      OffsetRecord(Time(math.max(ssc.graph.startTime.milliseconds,
        offsetRecord.timestamp.milliseconds)), offsetRecord.offsets)
    } else {
      // query start startSeqs
      val startSeqs = eventHubClient.startSeqOfPartition(retryIfFail = false,
        eventhubNameAndPartitions.toList)
      OffsetRecord(Time(math.max(ssc.graph.startTime.milliseconds,
        offsetRecord.timestamp.milliseconds)), offsetRecord.offsets.map{
        case (ehNameAndPartition, (offset, seq)) =>
          (ehNameAndPartition, (offset, startSeqs.get(ehNameAndPartition)))
        })
    }
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

  /**
   * return the last sequence number of each partition, which are to be received in this micro batch
   * @param startOffsetsAndSeqs the starting offset/seq of each partition for next batch
   * @param latestEndpoints the latest offset/seq of each partition
   */
  private def defaultRateControl(
      startOffsetsAndSeqs: Map[EventHubNameAndPartition, (Long, Long)],
      latestEndpoints: Map[EventHubNameAndPartition, (Long, Long)]):
    Map[EventHubNameAndPartition, Long] = {

    latestEndpoints.map{
      case (eventHubNameAndPar, (_, latestSeq)) =>
        val maximumAllowedMessageCnt = maxRateLimitPerPartition(eventHubNameAndPar.eventHubName)
        val endSeq = math.min(latestSeq,
          maximumAllowedMessageCnt + startOffsetsAndSeqs(eventHubNameAndPar)._2)
        (eventHubNameAndPar, endSeq)
    }
  }

  private def clamp(
      startOffsetsAndSeqs: Map[EventHubNameAndPartition, (Long, Long)],
      latestEndpoints: Map[EventHubNameAndPartition, (Long, Long)]):
    Map[EventHubNameAndPartition, Long] = {

    if (rateController.isEmpty) {
      defaultRateControl(startOffsetsAndSeqs, latestEndpoints)
    } else {
      val estimateRateLimit = rateController.map(_.getLatestRate().toInt)
      estimateRateLimit.filter(_ > 0) match {
        case None =>
          defaultRateControl(startOffsetsAndSeqs, latestEndpoints)
        case Some(allowedRate) =>
          val lagPerPartition = latestEndpoints.map {
            case (eventHubNameAndPartition, (_, latestSeq)) =>
              eventHubNameAndPartition ->
                math.max(latestSeq - startOffsetsAndSeqs(eventHubNameAndPartition)._2,
                  0)
          }
          val totalLag = lagPerPartition.values.sum
          lagPerPartition.map {
            case (eventHubNameAndPartition, lag) =>
              val backpressureRate = math.round(lag / totalLag.toFloat * allowedRate)
              eventHubNameAndPartition ->
                (backpressureRate + startOffsetsAndSeqs(eventHubNameAndPartition)._2)
          }
      }
    }
  }

  // we should only care about the passing offset types when we start for the first time of the
  // application or we just failed before we start processing any batch
  // a more expressive definition:
  // (!initialized && !ssc.isCheckpointPresent) || (ssc.isCheckpointPresent &&
  // currentOffsetsAndSeqNums.timestamp.milliseconds == -1)
  private def shouldCareEnqueueTimeOrOffset = currentOffsetsAndSeqNums.timestamp.milliseconds == -1


  private def validateFilteringParams(
      eventHubsClient: EventHubClient,
      eventhubsParams: Map[String, _],
      ehNameAndPartitions: List[EventHubNameAndPartition]): Unit = {
    // first check if the parameters are valid
    val latestEnqueueTimeOfPartitions = eventHubsClient.lastEnqueueTimeOfPartitions(
      retryIfFail = true, ehNameAndPartitions)
    require(latestEnqueueTimeOfPartitions.isDefined, "cannot get latest enqueue time from Event" +
      " Hubs Rest Endpoint")
    latestEnqueueTimeOfPartitions.get.foreach {
      case (ehNameAndPartition, latestEnqueueTime) =>
        val passInEnqueueTime = eventhubsParams.get(ehNameAndPartition.eventHubName) match {
          case Some(ehParams) =>
            ehParams.asInstanceOf[Map[String, String]].getOrElse(
              "eventhubs.filter.enqueuetime", Long.MinValue.toString).toLong
          case None =>
            eventhubsParams.asInstanceOf[Map[String, String]].getOrElse(
              "eventhubs.filter.enqueuetime", Long.MinValue.toString).toLong
        }
        require(latestEnqueueTime >= passInEnqueueTime,
          "you cannot pass in an enqueue time which is later than the highest enqueue time in" +
            s" event hubs, ($ehNameAndPartition, pass-in-enqueuetime $passInEnqueueTime," +
            s" latest-enqueuetime $latestEnqueueTime)")
    }
  }

  private def composeFromOffsetWithFilteringParams(
      eventhubsParams: Map[String, _],
      fetchedStartOffsetsInNextBatch: Map[EventHubNameAndPartition, (Long, Long)]):
    Map[EventHubNameAndPartition, (EventHubsOffsetType, Long)] = {

    fetchedStartOffsetsInNextBatch.map {
      case (ehNameAndPartition, (offset, seq)) =>
        val (offsetType, offsetStr) = EventHubsClientWrapper.configureStartOffset(
          offset.toString,
          eventhubsParams.get(ehNameAndPartition.eventHubName) match {
            case Some(ehConfig) =>
              ehConfig.asInstanceOf[Map[String, String]]
            case None =>
              eventhubsParams.asInstanceOf[Map[String, String]]
          })
        (ehNameAndPartition, (offsetType, offsetStr.toLong))
    }
  }

  private def calculateStartOffset(
      ehNameAndPartition: EventHubNameAndPartition,
      filteringOffsetAndType: Map[EventHubNameAndPartition, (EventHubsOffsetType, Long)],
      startOffsetInNextBatch: Map[EventHubNameAndPartition, (Long, Long)]):
    (EventHubsOffsetType, Long) = {

    filteringOffsetAndType.getOrElse(
      ehNameAndPartition,
      (EventHubsOffsetTypes.PreviousCheckpoint,
        startOffsetInNextBatch(ehNameAndPartition)._1)
    )
  }

  private def composeOffsetRange(
      startOffsetInNextBatch: OffsetRecord,
      highestOffsets: Map[EventHubNameAndPartition, (Long, Long)]): List[OffsetRange] = {
    val clampedSeqIDs = clamp(startOffsetInNextBatch.offsets, highestOffsets)
    // to handle filter.enqueueTime and filter.offset
    val filteringOffsetAndType = {
      if (shouldCareEnqueueTimeOrOffset) {
        // first check if the parameters are valid
        validateFilteringParams(eventHubClient, eventhubsParams,
          eventhubNameAndPartitions.toList)
        composeFromOffsetWithFilteringParams(eventhubsParams,
          startOffsetInNextBatch.offsets)
      } else {
        Map[EventHubNameAndPartition, (EventHubsOffsetType, Long)]()
      }
    }
    highestOffsets.map {
      case (eventHubNameAndPartition, (_, endSeqNum)) =>
        val (offsetType, offset) = calculateStartOffset(eventHubNameAndPartition,
          filteringOffsetAndType, startOffsetInNextBatch.offsets)
        OffsetRange(eventHubNameAndPartition,
          fromOffset = offset,
          fromSeq = startOffsetInNextBatch.offsets(eventHubNameAndPartition)._2,
          untilSeq = math.min(clampedSeqIDs(eventHubNameAndPartition), endSeqNum),
          offsetType = offsetType)
    }.toList
  }

  private def proceedWithNonEmptyRDD(
      validTime: Time,
      startOffsetInNextBatch: OffsetRecord,
      latestOffsetOfAllPartitions: Map[EventHubNameAndPartition, (Long, Long)]):
    Option[EventHubRDD] = {
    // normal processing
    validatePartitions(validTime, startOffsetInNextBatch.offsets.keys.toList)
    val offsetRanges = composeOffsetRange(startOffsetInNextBatch, latestOffsetOfAllPartitions)
    currentOffsetsAndSeqNums = startOffsetInNextBatch
    logInfo(s"starting batch at $validTime with $startOffsetInNextBatch")
    val eventHubRDD = new EventHubRDD(
      context.sparkContext,
      eventhubsParams,
      offsetRanges,
      validTime,
      OffsetStoreParams(progressDir, ssc.sparkContext.appName, this.id, eventHubNameSpace),
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

  private def failIfRestEndpointFail = fetchedHighestOffsetsAndSeqNums == null ||
    currentOffsetsAndSeqNums.offsets.equals(fetchedHighestOffsetsAndSeqNums.offsets)

  private def composeHighestOffset(validTime: Time) = {
    fetchLatestOffset(validTime, retryIfFail = failIfRestEndpointFail).orElse(
      {
        logWarning(s"failed to fetch highest offset at $validTime")
        if (failIfRestEndpointFail) {
          None
        } else {
          Some(fetchedHighestOffsetsAndSeqNums.offsets)
        }
      }
    )
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    if (!initialized) {
      ProgressTrackingListener.initInstance(ssc, progressDir)
    }
    require(progressTracker != null, "ProgressTracker hasn't been initialized")
    val highestOffsetOption = composeHighestOffset(validTime)
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
      val rdd = proceedWithNonEmptyRDD(validTime, startPointRecord, highestOffsetOption.get)
      initialized = true
      rdd
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    ProgressTracker.eventHubDirectDStreams += this
    initialized = false
  }

  private[eventhubs] class EventHubDirectDStreamCheckpointData(
      eventHubDirectDStream: EventHubDirectDStream) extends DStreamCheckpointData(this) {

    def batchForTime: mutable.HashMap[Time, Array[(EventHubNameAndPartition, Long, Long, Long,
      EventHubsOffsetType)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[(EventHubNameAndPartition, Long, Long, Long,
        EventHubsOffsetType)]]]
    }

    override def update(time: Time): Unit = {
      if (latestCheckpointTime == null || time > latestCheckpointTime) {
        latestCheckpointTime = time
      }
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val offsetRangeOfRDD = kv._2.asInstanceOf[EventHubRDD].offsetRanges.map(_.toTuple).toArray
        logInfo(s"update RDD ${offsetRangeOfRDD.mkString("[", ", ", "]")} at ${kv._1}")
        batchForTime += kv._1 -> offsetRangeOfRDD
      }
    }

    override def cleanup(time: Time): Unit = { }

    override def restore(): Unit = {
      // we have to initialize here, otherwise there is a race condition when recovering from spark
      // checkpoint
      logInfo("initialized ProgressTracker")
      ProgressTracker.initInstance(progressDir, context.sparkContext.appName,
        context.sparkContext.hadoopConfiguration)
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring EventHubRDD for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new EventHubRDD(
          context.sparkContext,
          eventhubsParams,
          b.map {case (ehNameAndPar, fromOffset, fromSeq, untilSeq, offsetType) =>
            OffsetRange(ehNameAndPar, fromOffset, fromSeq, untilSeq, offsetType)}.toList,
          t,
          OffsetStoreParams(progressDir, ssc.sparkContext.appName, id, eventHubNameSpace),
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
}

private[eventhubs] object EventHubDirectDStream {
  val cleanupLock = new Object
  var lastCleanupTime = -1L
}
