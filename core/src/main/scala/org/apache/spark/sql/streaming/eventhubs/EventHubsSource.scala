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

package org.apache.spark.sql.streaming.eventhubs

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.spark.eventhubscommon.{EventHubNameAndPartition, EventHubsConnector, OffsetRecord, RateControlUtils}
import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper, RestfulEventHubClient}
import org.apache.spark.eventhubscommon.rdd.{EventHubsRDD, OffsetRange, OffsetStoreParams}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source}
import org.apache.spark.sql.streaming.eventhubs.checkpoint.StructuredStreamingProgressTracker
import org.apache.spark.sql.types._

private[spark] class EventHubsSource(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    eventhubReceiverCreator: (Map[String, String], Int, Long, Int) => EventHubsClientWrapper =
      EventHubsClientWrapper.getEventHubReceiver,
    eventhubClientCreator: (String, Map[String, Map[String, String]]) => EventHubClient =
      RestfulEventHubClient.getInstance) extends Source with EventHubsConnector with Logging {

  case class EventHubsOffset(batchId: Long, offsets: Map[EventHubNameAndPartition, (Long, Long)])

  // the id of the stream which is mapped from eventhubs instance
  override val streamId: Int = EventHubsSource.streamIdGenerator.getAndIncrement()

  private val eventHubsNamespace: String = parameters("eventhubs.namespace")
  private val eventHubsName: String = parameters("eventhubs.name")

  require(eventHubsNamespace != null, "eventhubs.namespace is not defined")
  require(eventHubsName != null, "eventhubs.name is not defined")

  private var _eventHubsClient: EventHubClient = _

  private var _eventHubsReceiver: (Map[String, String], Int, Long, Int)
    => EventHubsClientWrapper = _

  private[eventhubs] def eventHubClient = {
    if (_eventHubsClient == null) {
      _eventHubsClient = eventhubClientCreator(eventHubsNamespace, Map(eventHubsName -> parameters))
    }
    _eventHubsClient
  }

  private[eventhubs] def eventHubsReceiver = {
    if (_eventHubsReceiver == null) {
      _eventHubsReceiver = eventhubReceiverCreator
    }
    _eventHubsReceiver
  }

  private[spark] val ehNameAndPartitions = {
    val partitionCount = parameters("eventhubs.partition.count").toInt
    (for (partitionId <- 0 until partitionCount)
      yield EventHubNameAndPartition(eventHubsName, partitionId)).toList
  }

  private implicit val cleanupExecutorService = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(1))

  // EventHubsSource is created for each instance of program, that means it is different with
  // DStream which will load the serialized Direct DStream instance from checkpoint
  StructuredStreamingProgressTracker.registeredConnectors += uid -> this

  // initialize ProgressTracker
  private val progressTracker = StructuredStreamingProgressTracker.initInstance(
    uid, parameters("eventhubs.progressTrackingDir"), sqlContext.sparkContext.appName,
    sqlContext.sparkContext.hadoopConfiguration)

  private[spark] def setEventHubClient(eventHubClient: EventHubClient): EventHubsSource = {
    _eventHubsClient = eventHubClient
    this
  }

  private[spark] def setEventHubsReceiver(
      eventhubReceiverCreator: (Map[String, String], Int, Long, Int) =>
        EventHubsClientWrapper): EventHubsSource = {
    _eventHubsReceiver = eventhubReceiverCreator
    this
  }

  // the flag to avoid committing in the first batch
  private[spark] var firstBatch = true
  // the offsets which have been to the self-managed offset store
  private[eventhubs] var committedOffsetsAndSeqNums: EventHubsOffset =
    EventHubsOffset(-1L, ehNameAndPartitions.map((_, (-1L, -1L))).toMap)
  // the highest offsets in EventHubs side
  private var fetchedHighestOffsetsAndSeqNums: EventHubsOffset = _

  override def schema: StructType = {
    EventHubsSourceProvider.sourceSchema(parameters)
  }

  private[spark] def composeHighestOffset(retryIfFail: Boolean):
      Option[Map[EventHubNameAndPartition, (Long, Long)]] = {
    RateControlUtils.fetchLatestOffset(eventHubClient,
      retryIfFail = retryIfFail,
      if (fetchedHighestOffsetsAndSeqNums == null) {
        null
      } else {
        fetchedHighestOffsetsAndSeqNums.offsets
      },
      committedOffsetsAndSeqNums.offsets) match {
      case Some(highestOffsets) =>
        fetchedHighestOffsetsAndSeqNums = EventHubsOffset(committedOffsetsAndSeqNums.batchId,
          highestOffsets)
        Some(fetchedHighestOffsetsAndSeqNums.offsets)
      case _ =>
        logWarning(s"failed to fetch highest offset")
        if (retryIfFail) {
          None
        } else {
          Some(fetchedHighestOffsetsAndSeqNums.offsets)
        }
    }
  }

  /**
   * when we have reached the end of the message queue in the remote end or we haven't get any
   * idea about the highest offset, we shall fail the app when rest endpoint is not responsive, and
   * to prevent us from dying too much, we shall retry with 2-power interval in this case
   */
  private def failAppIfRestEndpointFail = fetchedHighestOffsetsAndSeqNums == null ||
    committedOffsetsAndSeqNums.offsets.equals(fetchedHighestOffsetsAndSeqNums.offsets)


  private def cleanupFiles(batchIdToClean: Long): Unit = {
    Future {
      progressTracker.cleanProgressFile(batchIdToClean)
    }.onComplete {
      case Success(r) =>
        logInfo(s"finished cleanup for batch $batchIdToClean")
      case Failure(exception) =>
        logWarning(s"error happened when clean up for batch $batchIdToClean," +
          s" $exception")
    }
  }

  /**
   * there are two things to do in this function, first is to collect the ending offsets of last
   * batch, so that we know the starting offset of the current batch. And then, we calculate the
   * target seq number of the current batch
   *
   * @return return the target seqNum of current batch
   */
  override def getOffset: Option[Offset] = {
    val highestOffsetsOpt = composeHighestOffset(failAppIfRestEndpointFail)
    require(highestOffsetsOpt.isDefined, "cannot get highest offset from rest endpoint of" +
      " eventhubs")
    if (!firstBatch) {
      // committedOffsetsAndSeqNums.batchId is always no larger than the latest finished batch id
      val lastCommittedBatchId = committedOffsetsAndSeqNums.batchId
      collectFinishedBatchOffsetsAndCommit(lastCommittedBatchId + 1)
      // after the previous call the batch id of committedOffsetsAndSeqNums has been incremented
      // with one, we are safe to clean the progress files which have been committed before the last
      // finished batch
      cleanupFiles(lastCommittedBatchId)
    } else {
      firstBatch = false
    }
    val targetOffsets = RateControlUtils.clamp(committedOffsetsAndSeqNums.offsets,
      highestOffsetsOpt.get, parameters)
    Some(EventHubsBatchRecord(committedOffsetsAndSeqNums.batchId + 1,
      targetOffsets.map{case (ehNameAndPartition, seqNum) =>
        (ehNameAndPartition, math.min(seqNum,
          fetchedHighestOffsetsAndSeqNums.offsets(ehNameAndPartition)._2))}))
  }

  /**
   * collect the ending offsets/seq from executors to driver and commit
   */
  private[eventhubs] def collectFinishedBatchOffsetsAndCommit(committedBatchId: Long): Unit = {
    committedOffsetsAndSeqNums = fetchEndingOffsetOfLastBatch(committedBatchId)
    // we have two ways to handle the failure of commit and precommit:
    // First, we will validate the progress file and overwrite the corrupted progress file when
    // progressTracker is created; Second, to handle the case that we fail before we create
    // a file, we need to read the latest progress file in the directory and see if we have commit
    // the offsests (check if the timestamp matches) and then collect the files if necessary
    progressTracker.commit(Map(uid -> committedOffsetsAndSeqNums.offsets), committedBatchId)
    logInfo(s"committed offsets of batch $committedBatchId, collectedCommits:" +
      s" $committedOffsetsAndSeqNums")
  }

  private def fetchEndingOffsetOfLastBatch(committedBatchId: Long) = {
    val startOffsetOfUndergoingBatch = progressTracker.collectProgressRecordsForBatch(
      committedBatchId)
    if (startOffsetOfUndergoingBatch.isEmpty) {
      // first batch, take the initial value of the offset, -1
      EventHubsOffset(committedBatchId, committedOffsetsAndSeqNums.offsets)
    } else {
      EventHubsOffset(committedBatchId,
        startOffsetOfUndergoingBatch.filter { case (connectorUID, _) =>
          connectorUID == uid
        }.values.head.filter(_._1.eventHubName == parameters("eventhubs.name")))
    }
  }

  private def buildEventHubsRDD(endOffset: EventHubsBatchRecord): EventHubsRDD = {
    val offsetRanges = fetchedHighestOffsetsAndSeqNums.offsets.map {
      case (eventHubNameAndPartition, (_, endSeqNum)) =>
        OffsetRange(eventHubNameAndPartition,
          fromOffset = committedOffsetsAndSeqNums.offsets(eventHubNameAndPartition)._1,
          fromSeq = committedOffsetsAndSeqNums.offsets(eventHubNameAndPartition)._2,
          untilSeq = endOffset.targetSeqNums(eventHubNameAndPartition))
    }.toList
    new EventHubsRDD(
      sqlContext.sparkContext,
      Map(parameters("eventhubs.name") -> parameters),
      offsetRanges,
      committedOffsetsAndSeqNums.batchId + 1,
      OffsetStoreParams(parameters("eventhubs.progressTrackingDir"),
        streamId, uid = uid, subDirs = sqlContext.sparkContext.appName, uid),
      eventHubsReceiver
    )
  }

  private def convertEventHubsRDDToDataFrame(eventHubsRDD: EventHubsRDD): DataFrame = {
    import scala.collection.JavaConverters._
    val (containsProperties, userDefinedKeys) =
      EventHubsSourceProvider.ifContainsPropertiesAndUserDefinedKeys(parameters)
    val rowRDD = eventHubsRDD.map(eventData =>
      Row.fromSeq(Seq(eventData.getBytes, eventData.getSystemProperties.getOffset.toLong,
        eventData.getSystemProperties.getSequenceNumber,
        eventData.getSystemProperties.getEnqueuedTime.getEpochSecond,
        eventData.getSystemProperties.getPublisher,
        eventData.getSystemProperties.getPartitionKey
      ) ++ {
        if (containsProperties) {
          if (userDefinedKeys.nonEmpty) {
            userDefinedKeys.map(k => {
              eventData.getProperties.asScala.getOrElse(k, "").toString
            })
          } else {
            Seq(eventData.getProperties.asScala.map { case (k, v) => k -> v.toString })
          }
        } else {
          Seq()
        }
      }
      ))
    sqlContext.createDataFrame(rowRDD, schema)
  }

  private def validateReadResults(readProgress: OffsetRecord): Boolean = {
    readProgress.offsets.keySet == connectedInstances.toSet
  }

  private def readProgress(batchId: Long): EventHubsOffset = {
    val nextBatchId = batchId + 1
    val progress = progressTracker.read(uid, nextBatchId, fallBack = false)
    if (progress.timestamp.milliseconds == -1 || !validateReadResults(progress)) {
      // next batch hasn't been committed
      val lastCommittedOffset = progressTracker.read(uid, batchId, fallBack = false)
      EventHubsOffset(batchId, lastCommittedOffset.offsets)
    } else {
      EventHubsOffset(nextBatchId, progress.offsets)
    }
  }

  private def recoverFromFailure(start: Option[Offset], end: Offset): Unit = {
    val recoveredCommittedBatchId = {
      if (start.isEmpty) {
        -1
      } else {
        start.map {
          case so: SerializedOffset =>
            val batchRecord = JsonUtils.partitionAndSeqNum(so.json)
            batchRecord.asInstanceOf[EventHubsBatchRecord].batchId
          case batchRecord: EventHubsBatchRecord =>
            batchRecord.batchId
        }.get
      }
    }
    val latestProgress = readProgress(recoveredCommittedBatchId)
    if (latestProgress.offsets.isEmpty && start.isDefined) {
      // we shall not commit when start is empty, otherwise, we will have a duplicate processing
      // of the first batch
      collectFinishedBatchOffsetsAndCommit(recoveredCommittedBatchId)
    } else {
      committedOffsetsAndSeqNums = latestProgress
    }
    logInfo(s"recovered from a failure, startOffset: $start, endOffset: $end")
    val highestOffsets = composeHighestOffset(failAppIfRestEndpointFail)
    require(highestOffsets.isDefined, "cannot get highest offsets when recovering from a failure")
    fetchedHighestOffsetsAndSeqNums = EventHubsOffset(committedOffsetsAndSeqNums.batchId,
      highestOffsets.get)
    firstBatch = false
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    if (firstBatch) {
      // in this case, we are just recovering from a failure; the committedOffsets and
      // availableOffsets are fetched from in populateStartOffset() of StreamExecution
      // convert (committedOffsetsAndSeqNums is in initial state)
      recoverFromFailure(start, end)
    }
    val eventhubsRDD = buildEventHubsRDD({
      end match {
        case so: SerializedOffset =>
          JsonUtils.partitionAndSeqNum(so.json)
        case batchRecord: EventHubsBatchRecord =>
          batchRecord
      }
    })
    convertEventHubsRDDToDataFrame(eventhubsRDD)
  }

  override def stop(): Unit = {}

  override def uid: String = s"${eventHubsNamespace}_${eventHubsName}_$streamId"

  // the list of eventhubs partitions connecting with this connector
  override def connectedInstances: List[EventHubNameAndPartition] = ehNameAndPartitions
}

private object EventHubsSource {
  val streamIdGenerator = new AtomicInteger(0)
}
