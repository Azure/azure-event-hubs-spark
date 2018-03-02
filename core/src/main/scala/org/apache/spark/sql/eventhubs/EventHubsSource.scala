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

package org.apache.spark.sql.eventhubs

import java.io._
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.eventhubs.client.Client
import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.eventhubs.{ EventHubsConf, NameAndPartition, _ }
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{
  HDFSMetadataLog,
  Offset,
  SerializedOffset,
  Source
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.unsafe.types.UTF8String

private[spark] class EventHubsSource private[eventhubs] (sqlContext: SQLContext,
                                                         options: Map[String, String],
                                                         clientFactory: (EventHubsConf => Client),
                                                         metadataPath: String)
    extends Source
    with Logging {

  import EventHubsConf._
  import EventHubsSource._

  private lazy val partitionCount: Int = ehClient.partitionCount

  private val ehConf = EventHubsConf.toConf(options)
  private val ehName = ehConf.name

  private val sc = sqlContext.sparkContext

  private val maxOffsetsPerTrigger =
    options.get(MaxEventsPerTriggerKey).map(_.toSequenceNumber)

  private var _client: Client = _
  private[spark] def ehClient = {
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
              val version =
                parseVersion(content.substring(0, indexOfNewLine), VERSION)
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
        // translate starting points within ehConf to sequence numbers
        val seqNos = ehClient.translate(ehConf, partitionCount).map {
          case (pId, seqNo) =>
            (NameAndPartition(ehName, pId), seqNo)
        }
        val offset = EventHubsSourceOffset(seqNos)
        metadataLog.add(0, offset)
        logInfo(s"Initial sequence numbers: $seqNos")
        offset
      }
      .partitionToSeqNos
  }

  private var currentSeqNos: Option[Map[NameAndPartition, SequenceNumber]] = None

  private var earliestSeqNos: Option[Map[NameAndPartition, SequenceNumber]] = None

  override def schema: StructType = EventHubsSourceProvider.eventHubsSchema

  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionSeqNos is initialized
    initialPartitionSeqNos

    // This contains an array of the following elements:
    // (partitionId, (earliestSeqNo, latestSeqNo)
    val earliestAndLatest = for {
      p <- 0 until partitionCount
      n = ehName
    } yield (p, ehClient.boundedSeqNos(p))

    // There is a possibility that data from EventHubs will
    // expire before it can be consumed from Spark. We collect
    // the earliest sequence numbers available in the service
    // here. In getBatch, we'll make sure our starting sequence
    // numbers are greater than or equal to the earliestSeqNos.
    // If not, we'll report possible data loss.
    earliestSeqNos = Some(earliestAndLatest.map {
      case (p, (e, _)) => NameAndPartition(ehName, p) -> e
    }.toMap)

    val latest = earliestAndLatest.map {
      case (p, (_, l)) => NameAndPartition(ehName, p) -> l
    }.toMap

    val seqNos: Map[NameAndPartition, SequenceNumber] = maxOffsetsPerTrigger match {
      case None =>
        latest
      case Some(limit) if currentSeqNos.isEmpty =>
        rateLimit(limit, initialPartitionSeqNos, latest)
      case Some(limit) =>
        rateLimit(limit, currentSeqNos.get, latest)
    }

    currentSeqNos = Some(seqNos)
    logDebug(s"GetOffset: ${seqNos.toSeq.map(_.toString).sorted}")

    Some(EventHubsSourceOffset(seqNos))
  }

  def fetchEarliestSeqNos(
      nameAndPartitions: Seq[NameAndPartition]): Map[NameAndPartition, SequenceNumber] = {
    (for {
      nameAndPartition <- nameAndPartitions
      seqNo = ehClient.earliestSeqNo(nameAndPartition.partitionId)
    } yield nameAndPartition -> seqNo).toMap
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: SequenceNumber,
      from: Map[NameAndPartition, SequenceNumber],
      until: Map[NameAndPartition, SequenceNumber]): Map[NameAndPartition, SequenceNumber] = {
    val fromNew = fetchEarliestSeqNos(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (nameAndPartition, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(nameAndPartition).orElse(fromNew.get(nameAndPartition)).flatMap { begin =>
          val size = end - begin
          logDebug(s"rateLimit $nameAndPartition size is $size")
          if (size > 0) Some(nameAndPartition -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (nameAndPartition, end) =>
          nameAndPartition -> sizes
            .get(nameAndPartition)
            .map { size =>
              val begin = from.getOrElse(nameAndPartition, fromNew(nameAndPartition))
              val prorate = limit * (size / total)
              logDebug(s"rateLimit $nameAndPartition prorated amount is $prorate")
              // Don't completely starve small topicpartitions
              val off = begin + (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
              logDebug(s"rateLimit $nameAndPartition new offset is $off")
              // Paranoia, make sure not to return an offset that's past end
              Math.min(end, off)
            }
            .getOrElse(end)
      }
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    initialPartitionSeqNos

    logInfo(s"getBatch called with start = $start and end = $end")
    val untilSeqNos = EventHubsSourceOffset.getPartitionSeqNos(end)
    // On recovery, getBatch wil be called before getOffset
    if (currentSeqNos.isEmpty) {
      currentSeqNos = Some(untilSeqNos)
    }
    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(sqlContext.sparkContext.emptyRDD,
                                                schema,
                                                isStreaming = true)
    }
    val fromSeqNos = start match {
      case Some(prevBatchEndOffset) =>
        EventHubsSourceOffset.getPartitionSeqNos(prevBatchEndOffset)
      case None =>
        // we need to
        initialPartitionSeqNos
    }
    if (earliestSeqNos.isEmpty) {
      earliestSeqNos = Some(fromSeqNos)
    }
    fromSeqNos.map {
      case (nAndP, seqNo) =>
        if (seqNo < currentSeqNos.get(nAndP)) {
          reportDataLoss(
            s"Starting seqNo $seqNo in partition ${nAndP.partitionId} of EventHub ${nAndP.ehName} " +
              s"is behind the earliest sequence number ${currentSeqNos.get(nAndP)} " +
              s"present in the service. Some events may have expired and been missed.")
          nAndP -> currentSeqNos.get(nAndP)
        } else {
          nAndP -> seqNo
        }
    }

    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilSeqNos.keySet.diff(fromSeqNos.keySet)
    val newPartitionSeqNos = fetchEarliestSeqNos(newPartitions.toSeq)
    if (newPartitionSeqNos.keySet != newPartitions) {
      // We cannot get fromOffsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionSeqNos.keySet)
      reportDataLoss(
        s"Cannot find earliest sequence numbers of $deletedPartitions. Some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionSeqNos")
    newPartitionSeqNos.filter(_._2 != 0).foreach {
      case (p, s) =>
        reportDataLoss(
          s"Added partition $p starts from $s instead of 0. Some data may have been missed")
    }

    val deletedPartitions = fromSeqNos.keySet.diff(untilSeqNos.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    val nameAndPartitions = untilSeqNos.keySet.filter { p =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionSeqNos.contains(p) || fromSeqNos.contains(p)
    }.toSeq
    logDebug("Partitions: " + nameAndPartitions.mkString(", "))

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = (for {
      np <- nameAndPartitions
      fromSeqNo = fromSeqNos
        .getOrElse(np, throw new IllegalStateException(s"$np doesn't have a fromSeqNo"))
      untilSeqNo = untilSeqNos(np)
      preferredLoc = if (numExecutors > 0) {
        Some(sortedExecutors(Math.floorMod(np.hashCode, numExecutors)))
      } else None
    } yield OffsetRange(np, fromSeqNo, untilSeqNo, preferredLoc)).filter { range =>
      if (range.untilSeqNo < range.fromSeqNo) {
        reportDataLoss(
          s"Partition ${range.nameAndPartition}'s sequence number was changed from " +
            s"${range.fromSeqNo} to ${range.untilSeqNo}, some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    val rdd = new EventHubsRDD(sc, ehConf, offsetRanges, clientFactory).map { ed =>
      InternalRow(
        UTF8String.fromBytes(ed.getBytes),
        ed.getSystemProperties.getOffset.toLong,
        ed.getSystemProperties.getSequenceNumber,
        DateTimeUtils.fromJavaTimestamp(
          new java.sql.Timestamp(ed.getSystemProperties.getEnqueuedTime.toEpochMilli)),
        UTF8String.fromString(ed.getSystemProperties.getPublisher),
        UTF8String.fromString(ed.getSystemProperties.getPartitionKey)
      )
    }

    logInfo(
      "GetBatch generating RDD of offset range: " +
        offsetRanges.sortBy(_.nameAndPartition.toString).mkString(", "))

    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = synchronized {
    ehClient.close()
  }

  /**
   * Logs a warning when data may have been missed.
   */
  private def reportDataLoss(message: String): Unit = {
    logWarning(message + s". $InstructionsForPotentialDataLoss")
  }
}

private[eventhubs] object EventHubsSource {
  val InstructionsForPotentialDataLoss =
    """
      |Some data may have been lost because they are not available in EventHubs any more; either the
      | data was aged out by EventHubs or the EventHubs instance may have been deleted before all the data in the
      | instance was processed.
    """.stripMargin

  private[eventhubs] val VERSION = 1

  def getSortedExecutorList(sc: SparkContext): Array[String] = {
    val bm = sc.env.blockManager
    bm.master
      .getPeers(bm.blockManagerId)
      .toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  private def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
    if (a.host == b.host) { a.executorId > b.executorId } else { a.host > b.host }
  }
}
