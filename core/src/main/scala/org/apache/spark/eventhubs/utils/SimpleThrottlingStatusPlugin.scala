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

import java.net.URI

import org.apache.spark.eventhubs.{ NameAndPartition, PartitionContext }
import org.apache.spark.eventhubs.rdd.OffsetRange
import org.apache.spark.internal.Logging
import scala.collection.mutable

class SimpleThrottlingStatusPlugin extends ThrottlingStatusPlugin with Logging {

  override def onBatchCreation(
      partitionContext: PartitionContext,
      nextBatchLocalId: Long,
      nextBatchOffsetRanges: Array[OffsetRange],
      partitionsThrottleFactor: mutable.Map[NameAndPartition, Double]): Unit = {
    log.info(s"New Batch with localId = ${nextBatchLocalId} has been created for " +
      s"partitionContext: ${partitionContext} with start and end offsets: ${nextBatchOffsetRanges} " +
      s"and partitions throttle factors: ${partitionsThrottleFactor}")
  }

  override def onPartitionsPerformanceStatusUpdate(
      partitionContext: PartitionContext,
      latestUpdatedBatchLocalId: Long,
      partitionsBatchSizes: Map[NameAndPartition, Int],
      partitionsBatchReceiveTimeMS: Map[NameAndPartition, Long],
      partitionsPerformancePercentages: Option[Map[NameAndPartition, Double]]): Unit = {
    log.info(
      s"Latest updated batch with localId = ${latestUpdatedBatchLocalId}  for partitionContext: ${partitionContext} " +
        s"received these information: Batch size: ${partitionsBatchSizes}, batch receive times in ms: " +
        s"${partitionsBatchReceiveTimeMS}, performance percentages: ${partitionsPerformancePercentages}.")
  }
}
