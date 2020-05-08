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

package org.apache.spark.eventhubs

import java.util.logging.Logger

import scala.collection.mutable
import scala.collection.breakOut
import org.apache.spark.eventhubs.rdd.OffsetRange
import org.apache.spark.internal.Logging


class PartitionsStatusTracker extends Logging{

  import PartitionsStatusTracker._

  // retrives the batchStatus object based on the local batchId
  private val batchesStatus = mutable.Map[Long, BatchStatus]()

  /**
   * retirives the local batchId for a pair of (NameAndPartition, SequenceNumber)
   * it's useful to accss the right batch when a performance metric message is
   * received for a parition-RequestSeqNo pair.
   * it's getting updated every time a batch is removed or added to the tracker
   */
  private val partitionSeqNoPairToBatchIdMap = mutable.Map[String, Long]()

  /**
   * Add a batch to the tracker by creating a BatchStatus object and adding it to the map.
   * Also, we add mappings from each partition-startSeqNo pair to the batchId in order to be able to retrive
   * the batchStatus object from the map when the performance metric message is received.
   * Not that we ignore partitions with batchSize zero (startSeqNo == latestSeqNo) since we won't recieve
   * any performance metric message for such partitions.
   */
  def addBatch(batchId: Long,
               offsetRanges: Array[OffsetRange]): Unit = {
    if(batchesStatus.contains(batchId)) {
      throw new IllegalStateException(
        s"Batch with local batch id = $batchId already exists in the partition status tracker. Can't be added again.")
    }
    // find partitions with a zero size batch.. No performance metric msg will be received for those partitions
    val isZeroSizeBatchPartition: Map[NameAndPartition, Boolean] =
      offsetRanges.map(range  => (range.nameAndPartition, (range.fromSeqNo == range.untilSeqNo)))(breakOut)

    // create the batchStatus tracker and add it to the map
    batchesStatus(batchId) = new BatchStatus(batchId, offsetRanges.map(range => {
      val np = range.nameAndPartition
      (np, new PartitionStatus(np, range.fromSeqNo, isZeroSizeBatchPartition(np)))
    })(breakOut))

    // add the mapping from partition-startSeqNo pair to the batchId ... ignore partitions with zero batch size
    offsetRanges.filter(r => !isZeroSizeBatchPartition(r.nameAndPartition)).foreach(range => {
      val key = partitionSeqNoKey(range.nameAndPartition, range.fromSeqNo)
      addPartitionSeqNoToBatchIdMapping(key, batchId)
    })
  }

  /**
   * Remove a batch from the tracker by finding and deleting the batchStatus object from the map.
   * Also, we remove mappings from each partition-startSeqNo pair to the batchId since this batch is old
   * and should not be updated even if we receive a performacne metric message for it later.
   * Not that we ignore partitions with batchSize zero (PartitionStatus.emptyBatch) since those haven't been
   * added to the mapping when we added the batch to the tracker. [See addBatch]
   */
  def removeBatch(batchId: Long): Unit = {
    if(!batchesStatus.contains(batchId)) {
      logInfo(s"Batch with local batchId = $batchId doesn't exist in the batch status tracker,  so it can't be removed.")
      return
    }
    // remove the mapping from partition-seqNo pair to the batchId (ignore partitions with empty batch size)
    val batchStatus = batchesStatus(batchId)
    batchStatus.paritionsStatus.filter(p => !p._2.emptyBatch).values.foreach(ps => {
      val key = partitionSeqNoKey(ps.nAndP, ps.requestSeqNo)
      removePartitionSeqNoToBatchIdMapping(key)
    })
    // remove the batchStatus tracker from the map
    batchesStatus.remove(batchId)
  }

  private def addPartitionSeqNoToBatchIdMapping(key: String, batchId: Long): Unit = {
    if(partitionSeqNoPairToBatchIdMap.contains(key)) {
      throw new IllegalStateException(s"Partition-startSeqNo pair $key is already mapped to the batchId = " +
        s"${partitionSeqNoPairToBatchIdMap.get(key)}, so cant be reassigned to batchId = $batchId")
    }
    partitionSeqNoPairToBatchIdMap(key) = batchId
  }

  private def removePartitionSeqNoToBatchIdMapping(key: String): Unit = {
    if(!partitionSeqNoPairToBatchIdMap.contains(key)) {
      throw new IllegalStateException(
        s"Partition-startSeqNo pair $key doesn't exist in the partitionSeqNoPairToBatchIdMap, so it can't be removed.")
    }
    partitionSeqNoPairToBatchIdMap.remove(key)
  }

  /**
   * return the batch id for a given parition-RequestSeqNo pair.
   * if the batch doesn't exist in the tracker, return BATCH_NOT_FOUND
   */
  private def getBatchIdForPartitionSeqNoPair(nAndP: NameAndPartition, seqNo: SequenceNumber): Long = {
    val key = partitionSeqNoKey(nAndP, seqNo)
    partitionSeqNoPairToBatchIdMap.getOrElse(key, BATCH_NOT_FOUND)
  }

  /**
   * update the partition perforamcne in the underlying batch based on the information received
   * from the executor node. This is a best effort logic, so if the batch doesn't exist in the
   * tracker simply assumes this is an old performance metric and ignores it.
   *
   * @param nAndP Name and Id of the partition
   * @param requestSeqNo requestSeqNo in the batch which help to identify the local batch id in combination with nAndP
   * @param batchSize number of  events received by this partition in the batch
   * @param receiveTimeInMillis time (in MS) that took the partition to received the events in the batch
   */
  def updatePartitionPerformance(nAndP: NameAndPartition,
                                 requestSeqNo: SequenceNumber,
                                 batchSize: Int,
                                 receiveTimeInMillis: Long): Unit = {
    // find the batchId based on partition-requestSeqNo pair in the partitionSeqNoPairToBatchIdMap ... ignore if it doesn't exist
    val batchId = getBatchIdForPartitionSeqNoPair(nAndP, requestSeqNo)
    if(batchId == BATCH_NOT_FOUND) {
      logInfo(s"Can't find the corresponding batchId for the partition-requestSeqNo pair ($nAndP, $requestSeqNo) " +
        s"in the partition status tracker. Assume the message is for an old batch, so ignore it.")
      return
    }
    // find the batch in batchesStatus and update the partition performacne in the batch
    // if it doesn't exist there should be an error adding/removing batches in the tracker
    if(!batchesStatus.contains(batchId)) {
      throw new IllegalStateException(
        s"Batch with local batch id = $batchId doesn't exist in the partition status tracker, while mapping " +
          s"from a partition-seqNo to this batchId exists in the partition status tracker.")
    }
    val batchStatus = batchesStatus(batchId)
    batchStatus.updatePartitionPerformance(nAndP, batchSize, receiveTimeInMillis)
  }

  /**
   * Checks the latest batch with enough updates and retruns the  perforamnce percentage for each partition as a
   * value between [0-1] where 0 means the partition is not responding and 1 means it's working wihtout any
   * perforamnce issue. This information can be used to adjust the batch size for each partition in the next batch.
   */
  def partitionsPerformancePercentage(): Option[Map[NameAndPartition, Double]] = {
    // if there is no batch in the tracker, return None
    if(batchesStatus.isEmpty) {
      logDebug(s"There is no batch in the tracker, so return None")
      None
    }
    else {
      // find the latest batch with enough updates
      // In Scala 2.13 we can use: val latestUpdatedBatch = batchesStatus.maxByOption(b => b._2.receivedEnoughUpdates)
      implicit val ordering = new Ordering[(Long, BatchStatus)] {
        override def compare(x: (Long, BatchStatus), y: (Long, BatchStatus)): Int = (x._1 - y._1).toInt
      }
      val batchesWithEnoughUpdates = batchesStatus.filter(b => b._2.receivedEnoughUpdates)
      val latestUpdatedBatch: Option[BatchStatus] =
        if (batchesWithEnoughUpdates.isEmpty) None else Some(batchesWithEnoughUpdates.max._2)


      latestUpdatedBatch match {
        case None => {
          logDebug(s"No batch has ${PartitionsStatusTracker.enoughUpdatesCount} partitions with updates (enough updates), " +
            s"so return None")
          None
        }
        case Some(batch) => {
          logDebug(s"Batch ${batch.batchId} is the latest batch with enough updates. Caculate and return its perforamnce.")
          batch.getPerformancePercentages
        }
      }
    }
  }

  /**
   * Clean up the tracker. This will be called when the source has been stopped
   */
  def cleanUp() = {
    batchesStatus.map(b => b._2.paritionsStatus.clear)
    batchesStatus.clear
    partitionSeqNoPairToBatchIdMap.clear
  }

  /**
   * This methods i being used for testing
   */
  def batchIdsInTracker: scala.collection.Set[Long] = {
    this.batchesStatus.keySet
  }
}

object PartitionsStatusTracker {
  private val _partitionsStatusTrackerInstance = new PartitionsStatusTracker
  val BATCH_NOT_FOUND: Long = -1
  var partitionsCount: Int = 1
  var receiverTimeOutInMillis: Long = 60000
  var enoughUpdatesCount: Int = 1

  def setDefaultValuesInTracker(numOfPartitions: Int, ehName: String, receiverMaxTime: Long) = {
    partitionsCount = numOfPartitions
    receiverTimeOutInMillis = receiverMaxTime
    enoughUpdatesCount = (partitionsCount/2) + 1
  }

  private def partitionSeqNoKey(nAndP: NameAndPartition, seqNo: SequenceNumber): String =
    s"(name=${nAndP.ehName},pid=${nAndP.partitionId},startSeqNo=$seqNo)".toLowerCase

  def getPartitionStatusTracker: PartitionsStatusTracker = _partitionsStatusTrackerInstance
}


private[eventhubs] class BatchStatus(val batchId: Long,
                                     val paritionsStatus: mutable.Map[NameAndPartition, PartitionStatus]) extends Logging{

  private var hasEnoughUpdates: Boolean = false

  private var performancePercentages:Option[Map[NameAndPartition, Double]] = None

  def updatePartitionPerformance(nAndP: NameAndPartition,
                                 batchSize: Int,
                                 receiveTimeInMillis: Long): Unit = {
    if(!paritionsStatus.contains(nAndP)) {
      throw new IllegalStateException(
        s"Partition $nAndP doesn't exist in the batch status for batchId $batchId. This is an illegal state that shouldn't happen.")
    }
    paritionsStatus(nAndP).updatePerformanceMetrics(batchSize, receiveTimeInMillis)
  }

  def receivedEnoughUpdates : Boolean = {
    if(!hasEnoughUpdates) {
      hasEnoughUpdates = paritionsStatus.values.filter(par => par.hasBeenUpdated).size >= PartitionsStatusTracker.enoughUpdatesCount
    }
    hasEnoughUpdates
  }

  def getPerformancePercentages: Option[Map[NameAndPartition, Double]] = performancePercentages match {
      case Some(completedPerformancePercentages) => performancePercentages
      case None => {
        // just use partitions which have batchSize > 0 and have been updated
        val partitionsTimePerEvent = paritionsStatus.filter(p =>
          (p._2.hasBeenUpdated && !p._2.emptyBatch)).values.map(ps => ps.timePerEventInMillis)

        // check if there is no updated partition with batchSize > 0
        if(partitionsTimePerEvent.isEmpty) {
          logDebug(s"There is no updated partition with batchSize greater than 0 in batch $batchId, " +
            s"so return None ")
          None
        }
        else {
          // calculate the standard deviation
          val avgTimePerEvent: Double = partitionsTimePerEvent.sum.toDouble / partitionsTimePerEvent.size
          val stdDevTimePerEvent: Double = math.sqrt(
            partitionsTimePerEvent.map(_.toDouble).map(time => math.pow(time - avgTimePerEvent, 2)).sum / partitionsTimePerEvent.size)
          // average + standard deviation can't go beyond the receiver timeout
          logDebug(s"Calculated the average time per event = $avgTimePerEvent and the standard deviation = $stdDevTimePerEvent" +
            s" for updated partitions in the batch $batchId.")
          assert(avgTimePerEvent + stdDevTimePerEvent <= PartitionsStatusTracker.receiverTimeOutInMillis)

          // update performance metrics in each paritition and return that mapping
          paritionsStatus.foreach(par => par._2.updatePerformancePercentage(avgTimePerEvent, stdDevTimePerEvent))
          val ppp: Map[NameAndPartition, Double] = paritionsStatus.map(par => (par._1, par._2.performancePercentage))(breakOut)
          // if all partitions have been updated, save the result in performancePercentages
          if(paritionsStatus.values.filter(ps => ps.hasBeenUpdated).size == PartitionsStatusTracker.partitionsCount) {
            performancePercentages = Some(ppp)
          }
          Some(ppp)
      }
    }
  }

  override def toString: String = {
    s"BatchStatus(localBatchId=$batchId, PartitionsStatus=${paritionsStatus.values.toString()})"
  }
}

private[eventhubs] class PartitionStatus(val nAndP: NameAndPartition,
                                         val requestSeqNo: SequenceNumber,
                                         val emptyBatch: Boolean)  extends Logging{

  // a partition with an empty batch (batchSize = 0) doesn't receive any update message from the executor
  var hasBeenUpdated: Boolean = if(emptyBatch) true else false

  var performancePercentage: Double = 1

  private var batchSize: Int = if(emptyBatch) 0 else -1
  // total receive time for the batch in milli seconds
  private var batchReceiveTimeInMillis: Long = if(emptyBatch) 0 else -1

  var timePerEventInMillis: Double = if(emptyBatch) 0 else -1

  // Update the status of this partition with the received performance metrics
  def updatePerformanceMetrics(bSize: Int, receiveTimeInMillis: Long): Any = {
    this.batchSize = bSize
    this.batchReceiveTimeInMillis = receiveTimeInMillis
    this.hasBeenUpdated = true
    if(batchSize != 0)
      this.timePerEventInMillis = this.batchReceiveTimeInMillis.toDouble / this.batchSize
    logDebug(s"updatePerformanceMetrics for partition = $nAndP with request sequence number = $requestSeqNo contains" +
      s" batchSize = $batchSize and total receive time(ms) = $batchReceiveTimeInMillis")
  }

  def updatePerformancePercentage(averageTimePerEvent: Double, standardDeviation: Double): Unit = {
    val averagePlusStdDev: Double = averageTimePerEvent + standardDeviation
    if(!emptyBatch && hasBeenUpdated){
      if(timePerEventInMillis > averagePlusStdDev) {
        performancePercentage = averageTimePerEvent / timePerEventInMillis
      }
    }
  }

  override def toString: String = {
    val partitionInfo: String = s"(${nAndP.ehName}/${nAndP.partitionId}/$requestSeqNo)"
    if(hasBeenUpdated)
      s"PartitionStatus[$partitionInfo -> (batchSize=$batchSize, time(ms)=$batchReceiveTimeInMillis)]"
    else
      s"PartitionStatus[$partitionInfo -> (No Update)]"
  }
}