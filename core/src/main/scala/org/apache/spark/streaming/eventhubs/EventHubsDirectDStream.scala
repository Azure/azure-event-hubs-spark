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
import org.apache.spark.eventhubs.common._
import org.apache.spark.eventhubs.common.client.{ Client, EventHubsClientWrapper }
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ StreamingContext, Time }
import org.apache.spark.streaming.dstream.{ DStreamCheckpointData, InputDStream }
import org.apache.spark.streaming.scheduler.{ RateController, StreamInputInfo }
import org.apache.spark.streaming.scheduler.rate.RateEstimator

// TODO do we check for checkpoint data on startup? So far we're only checking in restore (line 140)

/**
 * A DStream where each EventHubs partition corresponds to an RDD partition.
 *
 * @param _ssc the StreamingContext this stream belongs to
 * @param ehConf the configurations related to your EventHubs. See [[EventHubsConf]] for detail.
 * @param clientFactory the factory method that creates an EventHubsClient
 */
private[spark] class EventHubsDirectDStream private[spark] (
    _ssc: StreamingContext,
    ehConf: EventHubsConf,
    clientFactory: (EventHubsConf => Client))
    extends InputDStream[EventData](_ssc)
    with Logging {
  require(ehConf.isValid)

  private lazy val partitionCount: Int = ehClient.partitionCount
  private lazy val ehName = ehConf.name.get

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

  protected def latestSeqNos(): Map[PartitionId, SequenceNumber] = {
    (for {
      partitionId <- 0 until partitionCount
      endPoint = ehClient.latestSeqNo(partitionId)
    } yield partitionId -> endPoint).toMap
  }

  protected def clamp(
      latestSeqNos: Map[PartitionId, SequenceNumber]): Map[PartitionId, SequenceNumber] = {
    (for {
      (partitionId, latestSeqNo) <- latestSeqNos
      maxRate = ehConf.maxRatesPerPartition.getOrElse(partitionId, DefaultMaxRatePerPartition)
      upperBound = math.min(fromSeqNos(partitionId) + maxRate, latestSeqNo)
      untilSeqNo = math.max(fromSeqNos(partitionId), upperBound)
    } yield partitionId -> untilSeqNo).toMap
  }

  override def compute(validTime: Time): Option[RDD[EventData]] = {
    val untilSeqNos = clamp(latestSeqNos())
    val offsetRanges = (for { partitionId <- 0 until partitionCount } yield
      OffsetRange(NameAndPartition(ehName, partitionId),
                  fromSeqNos(partitionId),
                  untilSeqNos(partitionId))).toArray

    val rdd = new EventHubsRDD(context.sparkContext, ehConf, offsetRanges, clientFactory)

    val description = offsetRanges.map(_.toString).mkString("\n")
    logInfo(s"Starting batch at $validTime for EH: $ehName with\n $description")

    val metadata =
      Map("seqNos" -> offsetRanges, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    //fromSeqNos = untilSeqNos.mapValues(_ + 1)
    fromSeqNos = untilSeqNos
    Some(rdd)
  }

  override def start(): Unit = {
    val concurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
    require(
      concurrentJobs == 1,
      "due to the limitation from eventhub, we do not allow to have multiple concurrent spark jobs")
    EventHubsClientWrapper.userAgent = s"Spark-Streaming-${ssc.sc.version}"
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
                                                 ehConf,
                                                 b.map(OffsetRange(_)),
                                                 clientFactory)
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
