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

import scala.collection.mutable
import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.SparkContext
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.client.Client
import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.client.EventHubsClient
import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.streaming.{ StreamingContext, Time }
import org.apache.spark.streaming.dstream.{ DStreamCheckpointData, InputDStream }
import org.apache.spark.streaming.scheduler.{ RateController, StreamInputInfo }
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
 * A DStream where each EventHubs partition corresponds to an RDD partition.
 * A partition on Event Hubs will always generate an RDD partition with the
 * same partition id. For example, RDD partition with index 1 always
 * corresponds to Event Hubs partition 1.
 *
 * The option eventhubs.maxRatePerPartition set in the [[EventHubsConf]]
 * sets the upper bound of events that will be received in a batch for
 * a single partition.
 *
 * @param _ssc          the StreamingContext this stream belongs to
 * @param ehConf        the configurations related to your EventHubs. See [[EventHubsConf]] for detail.
 * @param clientFactory the factory method that creates an EventHubsClient
 */
private[spark] class EventHubsDirectDStream private[spark] (_ssc: StreamingContext,
                                                            ehConf: EventHubsConf,
                                                            clientFactory: EventHubsConf => Client)
    extends InputDStream[EventData](_ssc)
    with Logging {

  import EventHubsDirectDStream._

  private lazy val partitionCount: Int = ehClient.partitionCount
  private lazy val ehName = ehConf.name

  @transient private var _client: Client = _

  private[spark] def ehClient: Client = this.synchronized {
    if (_client == null) _client = clientFactory(ehConf)
    _client
  }

  private var fromSeqNos: Map[PartitionId, SequenceNumber] = _

  private def init(): Unit = {
    fromSeqNos = ehClient.translate(ehConf, partitionCount)
  }

  init()

  protected[streaming] override val checkpointData = new EventHubDirectDStreamCheckpointData

  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.sparkContext.conf)) {
      logWarning("rateController: BackPressure is not currently supported.")
    }
    None
  }

  protected def earliestAndLatest
    : (Map[PartitionId, SequenceNumber], Map[PartitionId, SequenceNumber]) = {
    val earliestAndLatest = ehClient.allBoundedSeqNos
    val earliest = earliestAndLatest.map {
      case (p, (e, _)) => p -> e
    }
    val latest = earliestAndLatest.map {
      case (p, (_, l)) => p -> l
    }
    (earliest, latest)
  }

  protected def clamp(
      latestSeqNos: Map[PartitionId, SequenceNumber]): Map[PartitionId, SequenceNumber] = {
    (for {
      (partitionId, latestSeqNo) <- latestSeqNos
      nAndP = NameAndPartition(ehConf.name, partitionId)
      defaultMaxRate = ehConf.maxRatePerPartition.getOrElse(DefaultMaxRatePerPartition)
      partitionRates = ehConf.maxRatesPerPartition.getOrElse(Map.empty)
      maxRate = partitionRates.getOrElse(nAndP, defaultMaxRate)
      upperBound = math.min(fromSeqNos(partitionId) + maxRate, latestSeqNo)
      untilSeqNo = math.max(fromSeqNos(partitionId), upperBound)
    } yield partitionId -> untilSeqNo).toMap
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    val sortedExecutors = getSortedExecutorList(ssc.sparkContext)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    val (earliest, latest) = earliestAndLatest
    // Make sure our fromSeqNos are greater than or equal
    // to the earliest event in the service.
    fromSeqNos = fromSeqNos.map {
      case (p, seqNo) =>
        if (earliest(p) > seqNo) p -> earliest(p) else p -> seqNo
    }
    val untilSeqNos = clamp(latest)
    val offsetRanges = (for {
      p <- 0 until partitionCount
      preferredLoc = if (numExecutors > 0) {
        Some(sortedExecutors(Math.floorMod(NameAndPartition(ehName, p).hashCode, numExecutors)))
      } else None
    } yield
      OffsetRange(NameAndPartition(ehName, p), fromSeqNos(p), untilSeqNos(p), preferredLoc)).toArray

    val rdd = new EventHubsRDD(context.sparkContext, ehConf.trimmed, offsetRanges)

    val description = offsetRanges.map(_.toString).mkString("\n")
    logInfo(s"Starting batch at $validTime for EH: $ehName with\n$description")

    val metadata =
      Map("seqNos" -> offsetRanges, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    fromSeqNos = untilSeqNos
    Some(rdd)
  }

  override def start(): Unit = {
    EventHubsClient.userAgent = s"Spark-Streaming-$SparkConnectorVersion-${ssc.sc.version}"
  }

  override def stop(): Unit = {
    logInfo("stop: stopping EventHubDirectDStream")
    if (_client != null) _client.close()
  }

  private[eventhubs] class EventHubDirectDStreamCheckpointData extends DStreamCheckpointData(this) {

    import OffsetRange.OffsetRangeTuple

    def batchForTime: mutable.HashMap[Time, Array[OffsetRangeTuple]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRangeTuple]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[EventHubsRDD].offsetRanges.map(_.toTuple)
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, b) =>
          logInfo(s"Restoring EventHubsRDD for time $t ${b.mkString("[", ", ", "]")}")
          generatedRDDs += t -> new EventHubsRDD(context.sparkContext,
                                                 ehConf.trimmed,
                                                 b.map(OffsetRange(_)))
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

private[eventhubs] object EventHubsDirectDStream {
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
    if (a.host == b.host) {
      a.executorId > b.executorId
    } else {
      a.host > b.host
    }
  }
}
