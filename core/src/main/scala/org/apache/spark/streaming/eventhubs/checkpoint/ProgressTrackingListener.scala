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

package org.apache.spark.streaming.eventhubs.checkpoint

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubDirectDStream
import org.apache.spark.streaming.scheduler.{ StreamingListener, StreamingListenerBatchCompleted }

/**
 * The listener asynchronously commits the temp checkpoint to the path which is read by DStream
 * driver. It monitors the input size to prevent those empty batches from committing checkpoints
 */
private[eventhubs] class ProgressTrackingListener private (ssc: StreamingContext,
                                                           progressDirectory: String)
    extends StreamingListener
    with Logging {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logInfo(s"Batch ${batchCompleted.batchInfo.batchTime} completed")
    val batchTime = batchCompleted.batchInfo.batchTime.milliseconds
    try {
      if (batchCompleted.batchInfo.outputOperationInfos.forall(_._2.failureReason.isEmpty)) {
        val progressTracker =
          DirectDStreamProgressTracker.getInstance.asInstanceOf[DirectDStreamProgressTracker]
        // build current offsetsAndSeqNos
        val allEventDStreams = DirectDStreamProgressTracker.registeredConnectors
        // merge with the temp directory
        val startTime = System.currentTimeMillis()
        val progressInLastBatch =
          progressTracker.collectProgressRecordsForBatch(batchTime, allEventDStreams.toList)
        logInfo(s"progressInLastBatch $progressInLastBatch")
        if (progressInLastBatch.nonEmpty) {
          val contentToCommit = allEventDStreams
            .map {
              case dstream: EventHubDirectDStream =>
                (dstream.ehNamespace, dstream.currentOffsetsAndSeqNos.offsetsAndSeqNos)
            }
            .toMap
            .map {
              case (namespace, currentOffsets) =>
                (namespace, currentOffsets ++ progressInLastBatch.getOrElse(namespace, Map()))
            }
          progressTracker.commit(contentToCommit, batchTime)
          logInfo(
            s"commit ending offset of Batch $batchTime $contentToCommit time cost:" +
              s" ${System.currentTimeMillis() - startTime}")
        } else {
          logInfo(s"read RDD data from Checkpoint at $batchTime, skip commits")
        }
        ssc.graph.synchronized {
          ssc.graph.notifyAll()
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(s"listener thread is stopped due to ${e.getMessage}")
        throw new InterruptedException(s"stop listener thread for exception ${e.getMessage}")
    }
  }
}

private[eventhubs] object ProgressTrackingListener {

  private var _progressTrackerListener: ProgressTrackingListener = _

  private def getOrCreateProgressTrackerListener(ssc: StreamingContext,
                                                 progressDirectory: String) = {
    if (_progressTrackerListener == null) {
      _progressTrackerListener = new ProgressTrackingListener(ssc, progressDirectory)
      ssc.scheduler.listenerBus.listeners.add(0, _progressTrackerListener)
    }
    _progressTrackerListener
  }

  private[eventhubs] def reset(ssc: StreamingContext): Unit = {
    ssc.scheduler.listenerBus.listeners.remove(0)
    _progressTrackerListener = null
  }

  def initInstance(ssc: StreamingContext, progressDirectory: String): ProgressTrackingListener =
    this.synchronized {
      getOrCreateProgressTrackerListener(ssc, progressDirectory)
    }
}
