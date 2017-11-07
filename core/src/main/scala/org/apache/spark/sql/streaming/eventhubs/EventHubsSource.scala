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

import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange, ProgressTrackerParams }
import org.apache.spark.eventhubs.common.{ NameAndPartition, EventHubsConnector, RateControlUtils }
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{ Offset, SerializedOffset, Source }
import org.apache.spark.sql.streaming.eventhubs.checkpoint.StructuredStreamingProgressTracker
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

private[spark] class EventHubsSource private[eventhubs] (
    sqlContext: SQLContext,
    eventHubsParams: Map[String, String],
    clientFactory: (Map[String, String]) => Client)
    extends Source
    with EventHubsConnector
    with Logging {

  case class EventHubsOffset(batchId: Long, offsets: Map[NameAndPartition, (Long, Long)])

  override val streamId: Int = EventHubsSource.streamIdGenerator.getAndIncrement()

  private val ehNamespace = eventHubsParams("eventhubs.namespace")
  private val ehName = eventHubsParams("eventhubs.name")

  private var _client: Client = _
  private[eventhubs] def ehClient = {
    if (_client == null) {
      _client = clientFactory(eventHubsParams)
    }
    _client
  }
  private[spark] def ehClient_=(eventHubClient: Client) = {
    _client = eventHubClient
    this
  }

  private var _receiver: (Map[String, String]) => Client = _
  private[eventhubs] def receiverFactory = {
    if (_receiver == null) {
      _receiver = clientFactory
    }
    _receiver
  }
  private[spark] def receiverFactory_=(f: (Map[String, String]) => Client) = {
    _receiver = f
    this
  }

  override def namesAndPartitions: List[NameAndPartition] = {
    val partitionCount = eventHubsParams("eventhubs.partition.count").toInt
    (for (partitionId <- 0 until partitionCount)
      yield NameAndPartition(ehName, partitionId)).toList
  }

  private implicit val cleanupExecutorService =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  // EventHubsSource is created for each instance of program, that means it is different with
  // DStream which will load the serialized Direct DStream instance from checkpoint
  StructuredStreamingProgressTracker.registeredConnectors += uid -> this

  // initialize ProgressTracker
  private val progressTracker = StructuredStreamingProgressTracker.initInstance(
    uid,
    eventHubsParams("eventhubs.progressTrackingDir"),
    sqlContext.sparkContext.appName,
    sqlContext.sparkContext.hadoopConfiguration)

  // the flag to avoid committing in the first batch
  private[spark] var firstBatch = true

  // the offsetsAndSeqNos which have been to the self-managed offset store
  private[eventhubs] var committedOffsetsAndSeqNums: EventHubsOffset =
    EventHubsOffset(-1L, namesAndPartitions.map((_, (-1L, -1L))).toMap)

  // Highest Offsets and Sequence Numbers that exist in EventHubs (API call is made to get this)
  private var highestOffsetsAndSeqNums: EventHubsOffset =
    EventHubsOffset(-1L, namesAndPartitions.map((_, (-1L, -1L))).toMap)

  override def schema: StructType = EventHubsSourceProvider.sourceSchema(eventHubsParams)

  private def cleanupFiles(batchIdToClean: Long): Unit = {
    Future {
      progressTracker.cleanProgressFile(batchIdToClean)
    }.onComplete {
      case Success(_) =>
        logInfo(s"finished cleanup for batch $batchIdToClean")
      case Failure(exception) =>
        logWarning(s"error happened when clean up for batch $batchIdToClean, $exception")
    }
  }

  // TODO revisit this once RateControlUtils is cleaned up.
  def updatedHighest(): EventHubsOffset = {
    EventHubsOffset(
      committedOffsetsAndSeqNums.batchId,
      (for {
        nameAndPartition <- highestOffsetsAndSeqNums.offsets.keySet
        endPoint = ehClient.lastOffsetAndSeqNo(nameAndPartition)
      } yield nameAndPartition -> endPoint).toMap
    )
  }

  /**
   * there are two things to do in this function, first is to collect the ending offsetsAndSeqNos of last
   * batch, so that we know the starting offset of the current batch. And then, we calculate the
   * target seq number of the current batch
   *
   * @return return the target seqNum of current batch
   */
  override def getOffset: Option[Offset] = {
    highestOffsetsAndSeqNums = updatedHighest()
    if (!firstBatch) {
      // committedOffsetsAndSeqNums.batchId is always no larger than the latest finished batch id
      val lastCommittedBatchId = committedOffsetsAndSeqNums.batchId
      collectFinishedBatchOffsetsAndCommit(lastCommittedBatchId + 1)
      cleanupFiles(lastCommittedBatchId)
    } else {
      firstBatch = false
    }
    val targetOffsets = RateControlUtils.clamp(committedOffsetsAndSeqNums.offsets,
                                               highestOffsetsAndSeqNums.offsets.toList,
                                               eventHubsParams)
    Some(
      EventHubsBatchRecord(
        committedOffsetsAndSeqNums.batchId + 1,
        for { (nameAndPartition, seqNum) <- targetOffsets } yield
          (nameAndPartition,
           math.min(seqNum, highestOffsetsAndSeqNums.offsets(nameAndPartition)._2))
      ))
  }

  /**
   * collect the ending offsetsAndSeqNos/seq from executors to driver and commit
   */
  private[eventhubs] def collectFinishedBatchOffsetsAndCommit(committedBatchId: Long): Unit = {
    committedOffsetsAndSeqNums = fetchEndingOffsetOfLastBatch(committedBatchId)
    // we have two ways to handle the failure of commit and precommit:
    // First, we will validate the progress file and overwrite the corrupted progress file when
    // progressTracker is created; Second, to handle the case that we fail before we create
    // a file, we need to read the latest progress file in the directory and see if we have commit
    // the offsests (check if the timestamp matches) and then collect the files if necessary
    progressTracker.commit(Map(uid -> committedOffsetsAndSeqNums.offsets), committedBatchId)
    logInfo(
      s"committed offsetsAndSeqNos of batch $committedBatchId, collectedCommits: $committedOffsetsAndSeqNums")
  }

  private def fetchEndingOffsetOfLastBatch(committedBatchId: Long) = {
    val startOffsetOfUndergoingBatch =
      progressTracker.collectProgressRecordsForBatch(committedBatchId, List(this))
    if (startOffsetOfUndergoingBatch.isEmpty) {
      // first batch, take the initial value of the offset, -1
      EventHubsOffset(committedBatchId, committedOffsetsAndSeqNums.offsets)
    } else {
      EventHubsOffset(
        committedBatchId,
        startOffsetOfUndergoingBatch
          .filter {
            case (connectorUID, _) =>
              connectorUID == uid
          }
          .values
          .head
          .filter(_._1.ehName == eventHubsParams("eventhubs.name"))
      )
    }
  }

  private def composeOffsetRange(endOffset: EventHubsBatchRecord): List[OffsetRange] = {
    val filterOffsetAndType = {
      if (committedOffsetsAndSeqNums.batchId == -1) {
        val startSeqs = new mutable.HashMap[NameAndPartition, Long].empty
        for (nameAndPartition <- namesAndPartitions) {
          val seqNo = ehClient.earliestSeqNo(nameAndPartition).get
          startSeqs += nameAndPartition -> seqNo
        }

        committedOffsetsAndSeqNums = EventHubsOffset(-1, committedOffsetsAndSeqNums.offsets.map {
          case (ehNameAndPartition, (offset, _)) =>
            (ehNameAndPartition, (offset, startSeqs(ehNameAndPartition)))
        })
        RateControlUtils.validateFilteringParams(Map(ehName -> ehClient),
                                                 eventHubsParams,
                                                 namesAndPartitions)
        RateControlUtils.composeFromOffsetWithFilteringParams(eventHubsParams,
                                                              committedOffsetsAndSeqNums.offsets)
      } else {
        Map[NameAndPartition, (EventHubsOffsetType, Long)]()
      }
    }

    for {
      (nameAndPartition, seqNo) <- endOffset.targetSeqNums.toList
      (offsetType, fromOffset) = RateControlUtils.calculateStartOffset(
        nameAndPartition,
        filterOffsetAndType,
        committedOffsetsAndSeqNums.offsets)
    } yield
      (nameAndPartition,
       fromOffset,
       committedOffsetsAndSeqNums.offsets(nameAndPartition)._2,
       seqNo,
       offsetType)
  }

  private def buildEventHubsRDD(endOffset: EventHubsBatchRecord): EventHubsRDD = {
    val offsetRanges = composeOffsetRange(endOffset)
    new EventHubsRDD(
      sqlContext.sparkContext,
      Map(eventHubsParams("eventhubs.name") -> eventHubsParams),
      offsetRanges,
      committedOffsetsAndSeqNums.batchId + 1,
      ProgressTrackerParams(eventHubsParams("eventhubs.progressTrackingDir"),
                            streamId,
                            uid = uid,
                            subDirs = sqlContext.sparkContext.appName,
                            uid),
      receiverFactory
    )
  }

  private def convertEventHubsRDDToDataFrame(eventHubsRDD: EventHubsRDD): DataFrame = {
    import scala.collection.JavaConverters._
    val (containsProperties, userDefinedKeys) =
      EventHubsSourceProvider.ifContainsPropertiesAndUserDefinedKeys(eventHubsParams)
    val rowRDD = eventHubsRDD.map(
      eventData =>
        Row.fromSeq(Seq(
          eventData.getBytes,
          eventData.getSystemProperties.getOffset.toLong,
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
              Seq(eventData.getProperties.asScala.map {
                case (k, v) =>
                  k -> (if (v == null) null else v.toString)
              })
            }
          } else {
            Seq()
          }
        }))
    sqlContext.createDataFrame(rowRDD, schema)
  }

  private def readProgress(batchId: Long): EventHubsOffset = {
    val nextBatchId = batchId + 1
    val progress = progressTracker.read(uid, nextBatchId, fallBack = false)

    val validateReadResults = progress.offsetsAndSeqNos.keySet == namesAndPartitions.toSet
    if (progress.timestamp == -1 || !validateReadResults) {
      // next batch hasn't been committed successfully
      val lastCommittedOffset = progressTracker.read(uid, batchId, fallBack = false)
      EventHubsOffset(batchId, lastCommittedOffset.offsetsAndSeqNos)
    } else {
      EventHubsOffset(nextBatchId, progress.offsetsAndSeqNos)
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
    highestOffsetsAndSeqNums = updatedHighest()
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

  override def uid: String = s"${ehNamespace}_${ehName}_$streamId"
}

private object EventHubsSource {
  val streamIdGenerator = new AtomicInteger(0)
}
