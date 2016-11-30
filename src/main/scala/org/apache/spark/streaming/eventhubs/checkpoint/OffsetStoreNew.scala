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

trait OffsetStoreNew {

  private[checkpoint] def init(): Unit
  def write(time: Time,
            eventHubNameAndPartition: EventHubNameAndPartition, cpOffset: Long, cpSeq: Long): Unit
  def read(): Map[EventHubNameAndPartition, (Long, Long)]
  def commit(commitTime: Time): Unit
  def checkpointPath(): String
  def close(): Unit
}

object OffsetStoreNew {

  private[eventhubs] val streamIdToOffstore = new mutable.HashMap[Int, OffsetStoreNew]()

  def newInstance(checkpointDir: String, appName: String, streamId: Int, namespace: String,
                  hadoopConfiguration: Configuration, runOnExecutor: Boolean = false):
                  OffsetStoreNew = {
    val offsetStore =
      new DfsBasedOffsetStore2(checkpointDir, appName, streamId, namespace, hadoopConfiguration,
        !runOnExecutor)
    offsetStore.init()
    streamIdToOffstore += streamId -> offsetStore
    offsetStore
  }
}
