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

import java.time.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.SparkContext

private[spark] class PartitionPerformanceReceiver (override val rpcEnv: RpcEnv, val statusTracker: PartitionsStatusTracker) extends RpcEndpoint with Logging{

  override def onStart(): Unit = {
    logInfo("Start PartitionPerformanceReceiver RPC endpoint")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ppm: PartitionPerformanceMetric => {
      logDebug(s"Received PartitionPerformanceMetric $ppm")
      statusTracker.updatePartitionPerformance(ppm.nAndP, ppm.requestSeqNo, ppm.batchSize, ppm.receiveTimeInMillis)
    }
    case _ => {
      logError(s"Received an unknown message in PartitionPerformanceReceiver. It's not acceptable!")
    }
  }

  override def onStop(): Unit = {
    logInfo("Stop PartitionPerformanceReceiver RPC endpoint")
  }
}

case class PartitionPerformanceMetric(val nAndP: NameAndPartition,
                                      val executorId: String,
                                      val taskId: Long,
                                      val requestSeqNo: SequenceNumber,
                                      val batchSize: Int,
                                      val receiveTimeInMillis: Long) {

  override def toString: String = {
    s"Partition: $nAndP - ExecutorId: $executorId -  TaskId: $taskId - Request Seq. No. = $requestSeqNo - Batch Size = $batchSize - Elapsed Time(MS): $receiveTimeInMillis"
  }
}

private[spark] object PartitionPerformanceReceiver {
  val ENDPOINT_NAME = "PartitionPerformanceReceiver"
}