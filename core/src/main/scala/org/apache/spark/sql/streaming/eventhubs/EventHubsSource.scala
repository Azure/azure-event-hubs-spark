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

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.eventhubs.common._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

private[spark] class EventHubsSource private[eventhubs] (sqlContext: SQLContext,
                                                         options: Map[String, String],
                                                         clientFactory: (EventHubsConf => Client),
                                                         metadataPath: String,
                                                         failOnDataLoss: Boolean)
    extends Source
    with Logging {
  import EventHubsSource._

  private lazy val partitionCount: Int = ehClient.partitionCount

  private val ehConf = EventHubsConf.toConf(options)

  private val sc = sqlContext.sparkContext

  private val maxOffsetsPerTrigger =
    options.get("maxSeqNosPerTrigger").map(_.toSequenceNumber)

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
            (NameAndPartition(ehConf.name.get, pId), seqNo)
        }
        val offset = EventHubsSourceOffset(seqNos)
        metadataLog.add(0, offset)
        logInfo(s"Initial sequence numbers: $seqNos")
        offset
      }
      .partitionToSeqNos
  }

  private var currentSeqNos: Option[Map[NameAndPartition, SequenceNumber]] = None

  override def schema: StructType = EventHubsSourceProvider.sourceSchema(options)

  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionSeqNos is initialized
    initialPartitionSeqNos

    val latest = (for {
      p <- 0 until partitionCount
      n = ehConf.name.get
      nAndP = NameAndPartition(n, p)
      seqNo = ehClient.latestSeqNo(p)
    } yield NameAndPartition(n, p) -> seqNo).toMap

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
      seqNo = ehClient.earliestSeqNo(nameAndPartition)
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
    val fromSeqNos = start match {
      case Some(prevBatchEndOffset) =>
        EventHubsSourceOffset.getPartitionSeqNos(prevBatchEndOffset)
      case None =>
        // we need to
        initialPartitionSeqNos
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

    val partitions = untilSeqNos.keySet.filter { p =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionSeqNos.contains(p) || fromSeqNos.contains(p)
    }.toSeq
    logDebug("Partitions: " + partitions.mkString(", "))

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = (for {
      p <- partitions
      fromSeqNo = fromSeqNos.get(p).getOrElse {
        newPartitionSeqNos.getOrElse(p, {
          // This should never happen.
          throw new IllegalStateException(s"$p doesn't have a fromSeqNo")
        })
      }
      untilSeqNo = untilSeqNos(p)
      // preferredLoc - coming soon
    } yield OffsetRange(p, fromSeqNo, untilSeqNo)).filter { range =>
      if (range.untilSeqNo < range.fromSeqNo) {
        reportDataLoss(
          s"Partition ${range.nameAndPartition}'s sequence number was changed from " +
            s"${range.fromSeqNo} to ${range.untilSeqNo}, some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    val containsProperties =
      options.getOrElse("eventhubs.sql.containsProperties", "false").toBoolean
    val userDefinedKeys = {
      if (options.contains("eventhubs.sql.userDefinedKeys")) {
        options("eventhubs.sql.userDefinedKeys").split(",").toSeq
      } else {
        Seq()
      }
    }
    val rdd = new EventHubsRDD(
      sc,
      ehConf,
      offsetRanges,
      clientFactory
    ).map(
      eventData =>
        InternalRow.fromSeq(Seq(
          UTF8String.fromBytes(eventData.getBytes),
          eventData.getSystemProperties.getOffset.toLong,
          eventData.getSystemProperties.getSequenceNumber,
          eventData.getSystemProperties.getEnqueuedTime.toEpochMilli,
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

    logInfo(
      "GetBatch generating RDD of offset range: " +
        offsetRanges.sortBy(_.nameAndPartition.toString).mkString(", "))

    // On recovery, getBatch will get called before getOffset
    if (currentSeqNos.isEmpty) {
      currentSeqNos = Some(untilSeqNos)
    }

    sqlContext.internalCreateDataFrame(rdd, schema)
  }

  override def stop(): Unit = synchronized {
    ehClient.close()
  }

  /**
   * If 'failOnDataLoss' is true, this method will throw an 'IllegalStateException'.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $InstructionsForFailOnDataLossTrue")
    } else {
      logWarning(message + s". $InstructionsForFailOnDataLossFalse")
    }
  }
}

private[eventhubs] object EventHubsSource {
  val InstructionsForFailOnDataLossFalse =
    """
      |Some data may have been lost because they are not available in EventHubs any more; either the
      | data was aged out by EventHubs or the EventHubs instance may have been deleted before all the data in the
      | instance was processed. If you want your streaming query to fail on such cases, set the source
      | option "failOnDataLoss" to "true".
    """.stripMargin

  val InstructionsForFailOnDataLossTrue =
    """
      |Some data may have been lost because they are not available in EventHubs any more; either the
      | data was aged out by EventHubs or the EventHubs instance may have been deleted before all the data in the
      | instance was processed. If you don't want your streaming query to fail on such cases, set the
      | source option "failOnDataLoss" to "false".
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
