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
  EventHubsConnector,
  NameAndPartition,
  OffsetRecord,
  RateControlUtils
}
import org.apache.spark.eventhubs.common.client.{ Client, EventHubsClientWrapper }
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange, ProgressTrackerParams }
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
      logWarning("rateController: BackPressure is not currently supported.")
      None
      /*Some(new EventHubDirectDStreamRateController(id,
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

  // This contains the most recent *starting* offsetsAndSeqNos and sequence numbers that have been
  // used to make an RDD.
  private[eventhubs] var currentOffsetsAndSeqNos = OffsetRecord(-1L, {
    namesAndPartitions.map { ehNameAndSpace =>
      (ehNameAndSpace, (-1L, -1L))
    }.toMap
  })

  // We make API calls to the EventHubs service to get the highest offsetsAndSeqNos and
  // sequence numbers that exist in the service.
  private[eventhubs] var highestOffsetsAndSeqNos = OffsetRecord(-1L, {
    namesAndPartitions.map { ehNameAndSpace =>
      (ehNameAndSpace, (-1L, -1L))
    }.toMap
  })

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
  private def fetchStartOffsets(validTime: Time, fallBack: Boolean): OffsetRecord = {
    val offsetRecord = progressTracker.read(
      ehNamespace,
      validTime.milliseconds - ssc.graph.batchDuration.milliseconds,
      fallBack)
    require(offsetRecord.offsetsAndSeqNos.nonEmpty, "progress file cannot be empty")
    if (offsetRecord.timestamp != -1) {
      OffsetRecord(math.max(ssc.graph.startTime.milliseconds, offsetRecord.timestamp),
                   offsetRecord.offsetsAndSeqNos)
    } else {
      // query start startSeqs
      val startSeqs = new mutable.HashMap[NameAndPartition, Long].empty
      for (nameAndPartition <- namesAndPartitions) {
        val name = nameAndPartition.ehName
        val seqNo = ehClients(name).earliestSeqNo(nameAndPartition)
        require(seqNo.isDefined, s"Failed to get starting sequence number for $nameAndPartition")

        startSeqs += nameAndPartition -> seqNo.get
      }

      OffsetRecord(
        math.max(ssc.graph.startTime.milliseconds, offsetRecord.timestamp),
        offsetRecord.offsetsAndSeqNos.map {
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
        s"EventHubs: ${offsetRange.nameAndPartition}\t" +
          s"Starting offsetsAndSeqNos: ${offsetRange.fromOffset}" +
          s"SequenceNumber Range: ${offsetRange.fromSeq} to ${offsetRange.untilSeq}"
      }
      .mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata =
      Map("offsetsAndSeqNos" -> offsetRanges,
          StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, inputSize, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
  }

  // Old implementation was removed because there is no rate controller. Proper implementation
  // will be re-visited in the future.
  private def clamp(): Map[NameAndPartition, Long] = {
    RateControlUtils.clamp(currentOffsetsAndSeqNos.offsetsAndSeqNos,
                           highestOffsetsAndSeqNos.offsetsAndSeqNos.toList,
                           ehParams)
  }

  private def composeOffsetRange(
      currentOffsetsAndSeqNos: OffsetRecord,
      highestOffsetsAndSeqNos: List[(NameAndPartition, (Long, Long))]): List[OffsetRange] = {
    val clampedSeqNos = clamp()
    var filteringOffsetAndType = Map[NameAndPartition, (EventHubsOffsetType, Long)]()
    if (!initialized && !ssc.isCheckpointPresent) {
      RateControlUtils.validateFilteringParams(ehClients.toMap, ehParams, namesAndPartitions)
      filteringOffsetAndType = RateControlUtils.composeFromOffsetWithFilteringParams(
        ehParams,
        currentOffsetsAndSeqNos.offsetsAndSeqNos)
    }

    for {
      (nameAndPartition, (_, endSeqNo)) <- highestOffsetsAndSeqNos
      (offsetType, fromOffset) = RateControlUtils.calculateStartOffset(
        nameAndPartition,
        filteringOffsetAndType,
        currentOffsetsAndSeqNos.offsetsAndSeqNos)

    } yield
      (nameAndPartition,
       fromOffset,
       currentOffsetsAndSeqNos.offsetsAndSeqNos(nameAndPartition)._2,
       math.min(clampedSeqNos(nameAndPartition), endSeqNo),
       offsetType)
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    if (!initialized) ProgressTrackingListener.initInstance(ssc, progressDir)
    require(progressTracker != null, "ProgressTracker hasn't been initialized")

    currentOffsetsAndSeqNos = fetchStartOffsets(validTime, !initialized)
    while (currentOffsetsAndSeqNos.timestamp < validTime.milliseconds -
             ssc.graph.batchDuration.milliseconds) {
      logInfo(
        s"wait for ProgressTrackingListener to commit offsetsAndSeqNos at Batch" +
          s" ${validTime.milliseconds}")
      graph.wait()
      logInfo(s"wake up at Batch ${validTime.milliseconds} at DStream $id")
      currentOffsetsAndSeqNos = fetchStartOffsets(validTime, !initialized)
    }
    // At this point, currentOffsetsAndSeqNos is up to date. Now, we make API calls to the service
    // to retrieve the highest offsetsAndSeqNos and sequences numbers available.
    highestOffsetsAndSeqNos = OffsetRecord(
      validTime.milliseconds,
      (for {
        nameAndPartition <- highestOffsetsAndSeqNos.offsetsAndSeqNos.keySet
        name = nameAndPartition.ehName
        endPoint = ehClients(name).lastOffsetAndSeqNo(nameAndPartition)
      } yield nameAndPartition -> endPoint).toMap
    )

    logInfo(
      s"highestOffsetOfAllPartitions at $validTime: ${highestOffsetsAndSeqNos.offsetsAndSeqNos}")
    logInfo(s"$validTime currentOffsetTimestamp: ${currentOffsetsAndSeqNos.timestamp}")
    logInfo(s"starting batch at $validTime with $currentOffsetsAndSeqNos")
    require(namesAndPartitions.diff(currentOffsetsAndSeqNos.offsetsAndSeqNos.keys.toList).isEmpty,
            "proceedWithNonEmptyRDD: a partition is missing.")

    val offsetRanges =
      composeOffsetRange(currentOffsetsAndSeqNos, highestOffsetsAndSeqNos.offsetsAndSeqNos.toList)
    val eventHubRDD = new EventHubsRDD(
      context.sparkContext,
      ehParams,
      offsetRanges,
      validTime.milliseconds,
      ProgressTrackerParams(progressDir,
                            streamId,
                            uid = ehNamespace,
                            subDirs = ssc.sparkContext.appName),
      clientFactory
    )
    reportInputInto(validTime,
                    offsetRanges,
                    offsetRanges.map(ofr => ofr.untilSeq - ofr.fromSeq).sum.toInt)
    initialized = true
    Some(eventHubRDD)
  }

  override def start(): Unit = {
    val concurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
    require(
      concurrentJobs == 1,
      "due to the limitation from eventhub, we do not allow to have multiple concurrent spark jobs")
    DirectDStreamProgressTracker.initInstance(progressDir,
                                              context.sparkContext.appName,
                                              context.sparkContext.hadoopConfiguration)
    ProgressTrackingListener.initInstance(ssc, progressDir)
    EventHubsClientWrapper.userAgent = s"Spark-Streaming-${ssc.sc.version}"
  }

  override def stop(): Unit = {
    logInfo("stop: stopping EventHubDirectDStream")
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
            ProgressTrackerParams(progressDir, streamId, uid = ehNamespace, subDirs = appName),
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
