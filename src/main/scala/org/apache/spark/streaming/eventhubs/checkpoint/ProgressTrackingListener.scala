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
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

/**
 * The listener asynchronously commits the temp checkpoint to the path which is read by DStream
 * driver. It monitors the input size to prevent those empty batches from committing checkpoints
 */
private[eventhubs] class ProgressTrackingListener(progressDirectory: String, ssc: StreamingContext)
  extends StreamingListener with Logging {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    if (batchCompleted.batchInfo.outputOperationInfos.forall(_._2.failureReason.isEmpty)) {
      val progressTracker = ProgressTracker.getInstance(progressDirectory,
        ssc.sparkContext.appName,
        ssc.sparkContext.hadoopConfiguration)
      // build current offsets
      val allEventDStreams = EventHubDirectDStream.getDirectStreams
      allEventDStreams.map(dstream =>
        (dstream.eventHubNameSpace, dstream.currentOffsetsAndSeqNums)).toMap
      // merge with the temp directory
      val progressInLastBatch = progressTracker.snapshot()
      val contentToCommit = allEventDStreams.map {
        dstream => ((dstream.eventHubNameSpace, dstream.id), dstream.currentOffsetsAndSeqNums)
      }.toMap.map { case (namespace, currentOffsets) =>
        (namespace, currentOffsets ++ progressInLastBatch.getOrElse(namespace._1, Map()))
      }
      progressTracker.commit(contentToCommit, batchCompleted.batchInfo.batchTime.milliseconds)
    }
  }
}
