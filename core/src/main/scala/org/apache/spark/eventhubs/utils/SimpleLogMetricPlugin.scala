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

package org.apache.spark.eventhubs.utils

import org.apache.spark.eventhubs.{ NameAndPartition, TaskContextSlim }
import org.apache.spark.internal.Logging

class SimpleLogMetricPlugin extends MetricPlugin with Logging {
  override def onReceiveMetric(taskContextSlim: TaskContextSlim,
                               partitionInfo: NameAndPartition,
                               batchCount: Int,
                               batchSizeInBytes: Long,
                               elapsedTimeInMillis: Long): Unit = {
    log.info(s"[Receive] $taskContextSlim, eventhub partitionInfo: $partitionInfo, " +
      s"batchCount: $batchCount, batchSizeInBytes: $batchSizeInBytes, elapsed: $elapsedTimeInMillis (ms)")
  }

  override def onSendMetric(taskContextSlim: TaskContextSlim,
                            eventHubName: String,
                            batchCount: Int,
                            batchSizeInBytes: Long,
                            elapsedTimeInMillis: Long,
                            isSuccess: Boolean): Unit = {
    log.info(s"[Send] $taskContextSlim, eventhub name: $eventHubName, " +
      s"batchCount: $batchCount, batchSizeInBytes: $batchSizeInBytes, elapsed: $elapsedTimeInMillis (ms), isSuccess: $isSuccess")
  }
}
