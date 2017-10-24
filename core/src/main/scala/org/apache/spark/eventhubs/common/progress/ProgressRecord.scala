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

package org.apache.spark.eventhubs.common.progress

/**
 * this class represent the record written by ProgressWriter and read by ProgressTracker
 * this class is supposed to only be used by the classes within checkpoint package
 *
 * uid in DirectDStream refers to namespace and in Structured Streaming refers to
 * "namespace_name_streamid"
 *
 * timestamp in DirectDStream refers to the batch time, and in Structured Streaming refers to
 * BatchID
 *
 */
private[spark] case class ProgressRecord(timestamp: Long,
                                         uid: String,
                                         eventHubName: String,
                                         partitionId: Int,
                                         offset: Long,
                                         seqId: Long) {
  override def toString: String = {
    s"$timestamp $uid $eventHubName $partitionId $offset $seqId"
  }
}

private[spark] object ProgressRecord {

  def parse(line: String): Option[ProgressRecord] = {
    try {
      val Array(timestampStr, namespace, eventHubName, partitionIdStr, offsetStr, seqStr) =
        line.split(" ")
      Some(
        ProgressRecord(timestampStr.toLong,
                       namespace,
                       eventHubName,
                       partitionIdStr.toInt,
                       offsetStr.toLong,
                       seqStr.toLong))
    } catch {
      case m: RuntimeException =>
        m.printStackTrace()
        None
    }
  }
}
