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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import org.apache.commons.io.IOUtils
import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.eventhubs.common._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.execution.streaming.{
  HDFSMetadataLog,
  Offset,
  SerializedOffset,
  Source
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

private[spark] class EventHubsSource private[eventhubs] (sqlContext: SQLContext,
                                                         options: Map[String, String],
                                                         clientFactory: (EventHubsConf => Client),
                                                         metadataPath: String)
    extends Source
    with Logging {

  private val ehConf = EventHubsConf.toConf(options)
  private val sc = sqlContext.sparkContext

  private var _client: Client = _

  private[eventhubs] def ehClient = {
    if (_client == null) _client = clientFactory(ehConf)
    _client
  }

  private lazy val initialPartitionSeqNos = {
    val metadataLog =
      new HDFSMetadataLog[EventHubsSourceOffset](sqlContext.sparkSession, metadataPath) {
        override def serialize(metadata: EventHubsSourceOffset, out: OutputStream): Unit = {
          out.write(0) // SPARK-19517
          val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
          writer.write("v" + VERSION + "\n")
          writer.write(metadata.json)
          writer.flush()
        }

        override def deserialize(in: InputStream): EventHubsSourceOffset = {
          in.read() // zero byte is read (SPARK-19517)
          val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
          // HDFSMetadataLog guarantees that it never creates a partial file.
          assert(content.length != 0)
          if (content(0) == 'v') {
            val indexOfNewLine = content.indexOf("\n")
            if (indexOfNewLine > 0) {
              val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
              EventHubsSourceOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
            } else {
              throw new IllegalStateException("Log file was malformed.")
            }
          } else {
            EventHubsSourceOffset(SerializedOffset(content)) // Spark 2.1 log file
          }
        }
      }

    metadataLog
      .get(0)
      .getOrElse {
        val seqNos = ehClient.translate(ehConf).map {
          case (pId, seqNo) =>
            (NameAndPartition(ehConf.name.get, pId), seqNo)
        }
        val offset = EventHubsSourceOffset(seqNos)
        metadataLog.add(0, offset)
        logInfo(s"Initial sequence numbers: $seqNos")
        offset
      }
      .partitionToSeqNos
  }

  private implicit val cleanupExecutorService =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  // EventHubsSource is created for each instance of program, that means it is different with
  // DStream which will load the serialized Direct DStream instance from checkpoint
  StructuredStreamingProgressTracker.registeredConnectors += uid -> this

  // initialize ProgressTracker
  private val progressTracker = StructuredStreamingProgressTracker.initInstance(
    uid,
    ehConf("eventhubs.progressDirectory"),
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

  override def schema: StructType = EventHubsSourceProvider.sourceSchema(options)

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
        endPoint = ehClient.latestSeqNo(nameAndPartition)
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
                                               ehConf)
    Some(
      EventHubsSourceOffset(
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
          .filter(_._1.ehName == ehConf("eventhubs.name"))
      )
    }
  }

  private def composeOffsetRange(endOffset: EventHubsSourceOffset): List[OffsetRange] = {
    val filterOffsetAndType = {
      if (committedOffsetsAndSeqNums.batchId == -1) {
        val startSeqs = new mutable.HashMap[NameAndPartition, SequenceNumber].empty
        for (nameAndPartition <- namesAndPartitions) {
          val seqNo = ehClient.earliestSeqNo(nameAndPartition)
          startSeqs += nameAndPartition -> seqNo
        }

        committedOffsetsAndSeqNums = EventHubsOffset(-1, committedOffsetsAndSeqNums.offsets.map {
          case (ehNameAndPartition, (offset, _)) =>
            (ehNameAndPartition, (offset, startSeqs(ehNameAndPartition)))
        })
        RateControlUtils.validateFilteringParams(ehClient, ehConf, namesAndPartitions)
        RateControlUtils.composeFromOffsetWithFilteringParams(ehConf,
                                                              committedOffsetsAndSeqNums.offsets)
      } else {
        Map[NameAndPartition, (EventHubsOffsetType, SequenceNumber)]()
      }
    }

    for {
      (nameAndPartition, seqNo) <- endOffset.partitionToSeqNos.toList
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

  private def buildEventHubsRDD(endOffset: EventHubsSourceOffset): EventHubsRDD = {
    val offsetRanges = composeOffsetRange(endOffset)
    new EventHubsRDD(
      sqlContext.sparkContext,
      ehConf,
      offsetRanges,
      committedOffsetsAndSeqNums.batchId + 1,
      receiverFactory
    )
  }

  private def convertEventHubsRDDToDataFrame(eventHubsRDD: EventHubsRDD): DataFrame = {
    import scala.collection.JavaConverters._
    val containsProperties =
      options.getOrElse("eventhubs.sql.containsProperties", "false").toBoolean

    val userDefinedKeys = {
      if (options.contains("eventhubs.sql.userDefinedKeys")) {
        options("eventhubs.sql.userDefinedKeys").split(",").toSeq
      } else {
        Seq()
      }
    }

    val rowRDD = eventHubsRDD.map(
      eventData =>
        InternalRow.fromSeq(Seq(
          eventData.getBytes,
          eventData.getSystemProperties.getOffset.toLong,
          eventData.getSystemProperties.getSequenceNumber,
          eventData.getSystemProperties.getEnqueuedTime.getEpochSecond,
          UTF8String.fromString(eventData.getSystemProperties.getPublisher),
          UTF8String.fromString(eventData.getSystemProperties.getPartitionKey)
        ) ++ {
          if (containsProperties) {
            if (userDefinedKeys.nonEmpty) {
              userDefinedKeys.map(k => {
                UTF8String.fromString(eventData.getProperties.asScala.getOrElse(k, "").toString)
              })
            } else {
              val keys = ArrayBuffer[UTF8String]()
              val values = ArrayBuffer[UTF8String]()
              for ((k, v) <- eventData.getProperties.asScala) {
                keys.append(UTF8String.fromString(k))
                if (v == null) values.append(null)
                else values.append(UTF8String.fromString(v.toString))
              }
              Seq(ArrayBasedMapData(keys.toArray, values.toArray))
            }
          } else {
            Seq()
          }
        }))
    sqlContext.internalCreateDataFrame(rowRDD, schema)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    initialPartitionSeqNos

    logInfo(s"getBatch called with start = $start and end = $end")
    val untilSeqNos = EventHubsSourceOffset.getPartitionSeqNos(end)
    val fromSeqNos = start match {
      case Some(prevBatchEndOffset) =>
        EventHubsSourceOffset.getPartitionSeqNos(prevBatchEndOffset)
      case None =>
        initialPartitionSeqNos
    }

    // TODO: is there possibility for data loss? Will partitions ever be missing?
    val newPartitions = untilSeqNos.keySet.diff(fromSeqNos.keySet)
    val newPartitionSeqNos = (for {
      nameAndPartition <- newPartitions
      seqNo = ehClient.earliestSeqNo(nameAndPartition)
    } yield nameAndPartition -> seqNo).toMap
    assert(newPartitions == newPartitionSeqNos.keySet,
           s"Missing partitions: ${newPartitions.diff(newPartitionSeqNos.keySet)}")
    logInfo(s"Partitions added: $newPartitionSeqNos")

    // TODO: same as the one above.
    val deletedPartitions = fromSeqNos.keySet.diff(untilSeqNos.keySet)

    val eventhubsRDD = buildEventHubsRDD({
      end match {
        case so: SerializedOffset =>
          JsonUtils.partitionAndSeqNum(so.json)
        case batchRecord: EventHubsSourceOffset =>
          batchRecord
      }
    })
    convertEventHubsRDDToDataFrame(eventhubsRDD)
  }

  override def stop(): Unit = synchronized {
    ehClient.close()
  }
}

private[eventhubs] object EventHubsSource {
  private[eventhubs] val VERSION = 1
}
