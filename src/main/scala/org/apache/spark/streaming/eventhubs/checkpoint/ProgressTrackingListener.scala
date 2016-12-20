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

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubDirectDStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

/**
 * The listener asynchronously commits the temp checkpoint to the path which is read by DStream
 * driver. It monitors the input size to prevent those empty batches from committing checkpoints
 */
private[eventhubs] class ProgressTrackingListener private (
    ssc: StreamingContext, progressDirectory: String) extends StreamingListener with Logging {

  private val syncLatch: Object = new Serializable {}

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    if (batchCompleted.batchInfo.outputOperationInfos.forall(_._2.failureReason.isEmpty)) {
      val progressTracker = ProgressTracker.getInstance(ssc, progressDirectory,
        ssc.sparkContext.appName,
        ssc.sparkContext.hadoopConfiguration)
      // build current offsets
      val allEventDStreams = ProgressTrackingListener.eventHubDirectDStreams
      // merge with the temp directory
      val progressInLastBatch = progressTracker.snapshot()
      val contentToCommit = allEventDStreams.map {
        dstream => ((dstream.eventHubNameSpace, dstream.id), dstream.currentOffsetsAndSeqNums)
      }.toMap.map { case (namespace, currentOffsets) =>
        (namespace, currentOffsets ++ progressInLastBatch.getOrElse(namespace._1, Map()))
      }
      logInfo(s"latest offsets: $contentToCommit")
      syncLatch.synchronized {
        progressTracker.commit(contentToCommit, batchCompleted.batchInfo.batchTime.milliseconds)
        logInfo(s"commit offset at ${batchCompleted.batchInfo.batchTime}")
        // NOTE: we need to notifyAll here to handle multiple EventHubDirectStreams in application
        syncLatch.notifyAll()
      }
    }
  }
}

private[eventhubs] object ProgressTrackingListener {

  private var _progressTrackerListener: ProgressTrackingListener = _

  private[checkpoint] val eventHubDirectDStreams = new ListBuffer[EventHubDirectDStream]

  private def getOrCreateProgressTrackerListener(
      ssc: StreamingContext,
      progressDirectory: String,
      eventHubDirectDStream: EventHubDirectDStream) = {
    if (_progressTrackerListener == null) {
      _progressTrackerListener = new ProgressTrackingListener(ssc, progressDirectory)
      println("logging")
      if (eventHubDirectDStream != null) {
        ssc.addStreamingListener(_progressTrackerListener)
      }
    }
    eventHubDirectDStreams += eventHubDirectDStream
    _progressTrackerListener
  }

  private[checkpoint] def reset(): Unit = {
    _progressTrackerListener = null
  }

  def getInstance(ssc: StreamingContext,
                  progressDirectory: String,
                  eventHubDirectDStream: EventHubDirectDStream): ProgressTrackingListener =
    this.synchronized {
      getOrCreateProgressTrackerListener(ssc, progressDirectory, eventHubDirectDStream)
    }

  def getSyncLatch(
      ssc: StreamingContext,
      progressDirectory: String,
      eventHubDirectDStream: EventHubDirectDStream): Object = this.synchronized {
      getOrCreateProgressTrackerListener(ssc, progressDirectory, eventHubDirectDStream).syncLatch
  }
}

