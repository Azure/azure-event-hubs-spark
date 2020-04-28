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
import org.apache.spark.rpc.{RpcEnv, RpcEndpoint}
import org.apache.spark.SparkContext

private[spark] class PartitionPerformanceReceiver (override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging{

  private var maxBatchReceiveTime: Long = DefaultMaxBatchReceiveTime.toMillis

  def setMaxBatchReceiveTime(time: Duration) = {
    this.maxBatchReceiveTime = time.toMillis
  }

  override def onStart(): Unit = {
    logInfo("Start PartitionPerformanceReceiver RPC endpoint")
  }

  override def receive: PartialFunction[Any, Unit] = {
   // case PartitionPerformanceMetric(msg) => {
   //   logInfo(s"Receive PartitionPerformanceMetric with msg = $msg")
   // }
    case ppm: PartitionPerformanceMetric => {
      logInfo(s"Receive PartitionPerformanceMetric with msg $ppm")
    }
    case _ => {
      logInfo(s"Receive something other than PartitionPerformanceMetric in PartitionPerformanceReceiver. It's not acceptable!")
    }
  }

  override def onStop(): Unit = {
    logInfo("Stop PartitionPerformanceReceiver RPC endpoint")
  }

  // A Partition is considered to be slow if:
  // the total receive time for the current batch is beyond the MaxBatchReceiveTime
  private def isPartitionSlow(ppm: PartitionPerformanceMetric): Boolean = {
    if(ppm.receiveTimeInMillis > maxBatchReceiveTime)
      true
    else
      false
  }

}

//case class PartitionPerformanceMetric(msg: String)

case class PartitionPerformanceMetric(val nAndP: NameAndPartition,
                                      val executorId: String,
                                      val taskId: Long,
                                      val batchSize: Int,
                                      val receiveTimeInMillis: Long) {

  override def toString: String = {
    s"Partition: $nAndP - ExecutorId: $executorId -  TaskId: $taskId - Batch Size = $batchSize - Elapsed Time(MS): $receiveTimeInMillis"
  }
}

private[spark] object PartitionPerformanceReceiver {
  val ENDPOINT_NAME = "PartitionPerformanceReceiver"

}