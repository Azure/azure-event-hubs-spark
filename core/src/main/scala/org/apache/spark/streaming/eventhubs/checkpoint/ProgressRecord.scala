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

/**
 * this class represent the record written by ProgressWriter and read by ProgressTracker
 * this class is supposed to only be used by the classes within checkpoint package
 */
private[checkpoint] case class ProgressRecord(
    timestamp: Long, namespace: String, streamId: Int,
    eventHubName: String, partitionId: Int, offset: Long,
    seqId: Long) {
  override def toString: String = {
    s"$timestamp $namespace $streamId $eventHubName $partitionId $offset $seqId"
  }
}

private[checkpoint] object ProgressRecord {

  def parse(line: String): Option[ProgressRecord] = {
    try {
      val Array(timestampStr, namespace, streamId, eventHubName, partitionIdStr, offsetStr,
        seqStr) = line.split(" ")
      Some(ProgressRecord(timestampStr.toLong, namespace, streamId.toInt, eventHubName,
        partitionIdStr.toInt, offsetStr.toLong, seqStr.toLong))
    } catch {
      case m: RuntimeException =>
        m.printStackTrace()
        None
    }
  }
}
