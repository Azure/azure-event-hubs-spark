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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.Logging
import org.apache.spark.streaming.eventhubs.EventHubNameAndPartition

private[eventhubs] class ProgressWriter(
     progressDir: String,
     appName: String,
     streamId: Int,
     namespace: String,
     eventHubNameAndPartition: EventHubNameAndPartition,
     timestamp: Long,
     hadoopConfiguration: Configuration) extends Logging {

  private val tempProgressTrackingPointStr = PathTools.progressTempFileStr(
    PathTools.progressTempDirPathStr(progressDir, appName),
    streamId, namespace, eventHubNameAndPartition, timestamp)

  private[eventhubs] val tempProgressTrackingPointPath = new Path(tempProgressTrackingPointStr)

  def write(recordTime: Long, cpOffset: Long, cpSeq: Long): Unit = {
    val fs = tempProgressTrackingPointPath.getFileSystem(hadoopConfiguration)
    var cpFileStream: FSDataOutputStream = null
    try {
      // it would be safe to overwrite checkpoint, since we will not start a new job when
      // checkpoint hasn't been committed
      cpFileStream = fs.create(tempProgressTrackingPointPath, true)
      val record = ProgressRecord(recordTime, namespace, streamId,
        eventHubNameAndPartition.eventHubName, eventHubNameAndPartition.partitionId, cpOffset,
        cpSeq)
      cpFileStream.writeBytes(s"$record")
    } catch {
      case ioe: IOException =>
        ioe.printStackTrace()
        throw ioe
    } finally {
      if (cpFileStream != null) {
        cpFileStream.close()
      }
    }
  }
}


