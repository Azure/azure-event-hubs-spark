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

import com.microsoft.azure.eventhubs.{EventData, PartitionReceiver}

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
                                              EventHubsClientWrapper.getEventHubClient,
    eventhubClientCreator: (String, Map[String, Map[String, String]]) => EventHubClient =
                                              RestfulEventHubClient.getInstance)
  extends InputDStream[EventData](_ssc) with Logging {

  private[streaming] override def name: String = s"EventHub direct stream [$id]"

  private var initialized = false

  private var consumedAllMessages = false

  ProgressTracker.eventHubDirectDStreams += this

  protected[streaming] override val checkpointData = new EventHubDirectDStreamCheckpointData(this)

  private[eventhubs] val eventhubNameAndPartitions = {
    for (eventHubName <- eventhubsParams.keySet;
         partitionId <- 0 until eventhubsParams(eventHubName)(
      "eventhubs.partition.count").toInt) yield EventHubNameAndPartition(eventHubName, partitionId)
  }

  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.sparkContext.conf)) {
      Some(new EventHubDirectDStreamRateController(id, RateEstimator.create(ssc.sparkContext.conf,
        graph.batchDuration)))
    } else {
      None
    }
  }

  private def maxRateLimitPerPartition(eventHubName: String): Int = {
    val x = eventhubsParams(eventHubName).getOrElse("eventhubs.maxRate", "10000").toInt
    require(x > 0, s"eventhubs.maxRate has to be larger than zero, violated by $eventHubName ($x)")
    x
  }

  private var _eventHubClient: EventHubClient = _

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

  // from eventHubName to offset
  private[eventhubs] var currentOffsetsAndSeqNums = Map[EventHubNameAndPartition, (Long, Long)]()

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

  private def fetchLatestOffset(validTime: Time): Map[EventHubNameAndPartition, (Long, Long)] = {
    // TODO: rate control
    val latestOffsets = eventHubClient.endPointOfPartition()
    if (latestOffsets.isDefined) {
      latestOffsets.get
    } else {
      logError(s"EventHub $eventHubNameSpace Rest Endpoint is not responsive, will skip this" +
        s" micro batch and process it later")
      Map()
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
  private def fetchStartOffsetForEachPartition(validTime: Time):
    Map[EventHubNameAndPartition, (Long, Long)] = {
    val checkpoints = progressTracker.read(eventHubNameSpace, validTime.milliseconds)
    if (checkpoints.nonEmpty) {
      logInfo(s"get checkpoint at $validTime: $checkpoints")
      checkpoints
    } else {
      // start from the beginning of each eventHubPartition
      {
        for (eventHubName <- eventhubsParams.keys;
             p <- 0 until eventhubsParams(eventHubName)("eventhubs.partition.count").toInt)
          yield (EventHubNameAndPartition(eventHubName, p),
            (PartitionReceiver.START_OF_STREAM.toLong, -1L))
      }.toMap
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
    if (currentOffsetsAndSeqNums.nonEmpty) {
      val currentPartitions = currentOffsetsAndSeqNums.keys.toList
      val diff = currentPartitions.diff(calculatedPartitions)
      if (diff.nonEmpty) {
        logError(s"detected lost partitions $diff")
        throw new RuntimeException(s"some partitions are lost before $validTime")
      }
    }
  }

  /**
   * return the last sequence number of each partition, which are to be received in this micro batch
   * @param latestEndpoints the latest offset/seq of each partition
   */
  private def defaultRateControl(latestEndpoints: Map[EventHubNameAndPartition, (Long, Long)]):
      Map[EventHubNameAndPartition, Long] = {
    latestEndpoints.map{
      case (eventHubNameAndPar, (_, latestSeq)) =>
        val maximumAllowedMessageCnt = maxRateLimitPerPartition(eventHubNameAndPar.eventHubName)
        val endSeq = math.min(latestSeq,
          maximumAllowedMessageCnt + currentOffsetsAndSeqNums(eventHubNameAndPar)._2)
        (eventHubNameAndPar, endSeq)
    }
  }

  private def clamp(latestEndpoints: Map[EventHubNameAndPartition, (Long, Long)]):
      Map[EventHubNameAndPartition, Long] = {
    if (rateController.isEmpty) {
      defaultRateControl(latestEndpoints)
    } else {
      val estimateRateLimit = rateController.map(_.getLatestRate().toInt)
      estimateRateLimit.filter(_ > 0) match {
        case None =>
          defaultRateControl(latestEndpoints)
        case Some(allowedRate) =>
          val lagPerPartition = latestEndpoints.map {
            case (eventHubNameAndPartition, (_, latestSeq)) =>
              eventHubNameAndPartition ->
                math.max(latestSeq - currentOffsetsAndSeqNums(eventHubNameAndPartition)._2, 0)
          }
          val totalLag = lagPerPartition.values.sum
          lagPerPartition.map {
            case (eventHubNameAndPartition, lag) =>
              val backpressureRate = math.round(lag / totalLag.toFloat * allowedRate)
              eventHubNameAndPartition ->
                (backpressureRate + currentOffsetsAndSeqNums(eventHubNameAndPartition)._2)
          }
      }
    }
  }

  private def currentOffsetEarlierThanCheckpoint(
      offsetsMap2: Map[EventHubNameAndPartition, (Long, Long)]): Boolean = {
    currentOffsetsAndSeqNums.map {
      case (eh, (offset, seq)) => (eh, (offset, seq), (offsetsMap2(eh)._1, offsetsMap2(eh)._2))
    }.forall { case (_, (o1, s1), (o2, s2)) => o1 <= o2 && s1 <= s2 }
  }

  private def proceedWithNonEmptyRDD(
      validTime: Time,
      startOffsetInNextBatch: Map[EventHubNameAndPartition, (Long, Long)],
      latestOffsetOfAllPartitions: Map[EventHubNameAndPartition, (Long, Long)]):
    Option[EventHubRDD] = {
    // normal processing
    validatePartitions(validTime, startOffsetInNextBatch.keys.toList)
    currentOffsetsAndSeqNums = startOffsetInNextBatch
    val clampedSeqIDs = clamp(latestOffsetOfAllPartitions)
    logInfo(s"starting batch at $validTime with $startOffsetInNextBatch")
    val offsetRanges = latestOffsetOfAllPartitions.map {
      case (eventHubNameAndPartition, (_, endSeqNum)) =>
        OffsetRange(eventHubNameAndPartition,
          fromOffset = startOffsetInNextBatch(eventHubNameAndPartition)._1,
          fromSeq = startOffsetInNextBatch(eventHubNameAndPartition)._2,
          untilSeq = math.min(clampedSeqIDs(eventHubNameAndPartition), endSeqNum))
    }.toList
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

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    if (!initialized) {
      ProgressTrackingListener.initInstance(ssc, progressDir)
    }
    require(progressTracker != null, "ProgressTracker hasn't been initialized")
    val latestOffsetOfAllPartitions = fetchLatestOffset(validTime)
    println(s"latestOffsetOfAllPartitions: $latestOffsetOfAllPartitions")
    if (latestOffsetOfAllPartitions.isEmpty) {
      currentOffsetsAndSeqNums = fetchStartOffsetForEachPartition(validTime)
      consumedAllMessages = true
      Some(ssc.sparkContext.emptyRDD[EventData])
    } else {
      var startPointInNextBatch = fetchStartOffsetForEachPartition(validTime)
      while (startPointInNextBatch.equals(currentOffsetsAndSeqNums) &&
        !startPointInNextBatch.equals(latestOffsetOfAllPartitions) &&
        !consumedAllMessages &&
        initialized) {
        logInfo(s"wait for ProgressTrackingListener to commit offsets at Batch" +
          s" ${validTime.milliseconds}")
        graph.wait()
        logInfo(s"wake up at Batch ${validTime.milliseconds}")
        startPointInNextBatch = fetchStartOffsetForEachPartition(validTime)
      }
      // keep this state to prevent dstream dying
      if (startPointInNextBatch.equals(latestOffsetOfAllPartitions)) {
        consumedAllMessages = true
      } else {
        consumedAllMessages = false
      }
      initialized = true
      proceedWithNonEmptyRDD(validTime, startPointInNextBatch, latestOffsetOfAllPartitions)
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

    def batchForTime: mutable.HashMap[Time,
      Array[(EventHubNameAndPartition, Long, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time,
        Array[(EventHubNameAndPartition, Long, Long, Long)]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.filter(_._2.isInstanceOf[EventHubRDD]).foreach { kv =>
        val offsetRangeOfRDD = kv._2.asInstanceOf[EventHubRDD].offsetRanges.map(_.toTuple).toArray
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
        generatedRDDs += t -> new EventHubRDD(
          context.sparkContext,
          eventhubsParams,
          b.map {case (ehNameAndPar, fromOffset, fromSeq, untilSeq) =>
            OffsetRange(ehNameAndPar, fromOffset, fromSeq, untilSeq)}.toList,
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
