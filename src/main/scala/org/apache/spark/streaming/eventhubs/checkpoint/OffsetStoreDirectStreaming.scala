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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.eventhubs.EventHubNameAndPartition

// NOTE: since we have some logic to workaround the limitation from eventhubs, we shall not
// open this interface to the user to implement their own offsetstore for now
private[eventhubs] trait OffsetStoreDirectStreaming {

  /**
   * this method will be called when creating a new instance of OffsetStoreNew
   */
  private[checkpoint] def init(): Unit

  /**
   * write starting offset in the next batch to checkpoint
   * @param time the batch time
   * @param eventHubNameAndPartition the eventHubNameAndPartition
   * @param startingOffset the starting offset
   * @param startingSeq the staring sequence
   */
  def write(
      time: Time,
      eventHubNameAndPartition: EventHubNameAndPartition,
      startingOffset: Long,
      startingSeq: Long): Unit

  /**
   * read the checkpoint records
   * @return a map from eventhub-partition to (startOffset, startSeq)
   */
  def read(): Map[EventHubNameAndPartition, (Long, Long)]

  /**
   * commit checkpoint file
   * @param batchTime the batch time of the checkpoint file
   */
  def commit(batchTime: Time): Unit

  /**
   * clsoe checkpoint
   */
  def close(): Unit
}

private[eventhubs] object OffsetStoreDirectStreaming {

  private[eventhubs] val streamIdToOffstore = new mutable.HashMap[Int, OffsetStoreDirectStreaming]()

  def newInstance(checkpointDir: String, appName: String, streamId: Int, namespace: String,
                  hadoopConfiguration: Configuration, runOnDriver: Boolean = true):
                  OffsetStoreDirectStreaming = {
    val offsetStore =
      new DfsBasedOffsetStore2(checkpointDir, appName, streamId, namespace, hadoopConfiguration,
        runOnDriver)
    offsetStore.init()
    streamIdToOffstore += streamId -> offsetStore
    offsetStore
  }
}
