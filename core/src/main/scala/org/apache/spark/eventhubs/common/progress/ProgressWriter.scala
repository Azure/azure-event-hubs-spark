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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataOutputStream, Path }
import org.apache.spark.eventhubs.common.NameAndPartition
import org.apache.spark.internal.Logging

private[spark] class ProgressWriter(streamId: Int,
                                    uid: String,
                                    eventHubNameAndPartition: NameAndPartition,
                                    timestamp: Long,
                                    hadoopConfiguration: Configuration,
                                    progressDir: String,
                                    subDirIdentifiers: String*)
    extends Logging {

  // TODO: Why can't we get this info from one of the ProgressTrackers?
  // TODO: Come up with better name for this guy
  private val tempProgressTrackingPointStr =
    PathTools.makeTempDirectoryStr(progressDir, subDirIdentifiers: _*) + "/" +
      PathTools.makeTempFileName(streamId, uid, eventHubNameAndPartition, timestamp)

  // TODO: Why can't we get this info from one of the ProgressTrackers?
  // TODO: Come up with better name for this guy
  private[spark] val tempProgressTrackingPointPath = new Path(tempProgressTrackingPointStr)

  def write(recordTime: Long, cpOffset: Long, cpSeq: Long): Unit = {
    val fs = tempProgressTrackingPointPath.getFileSystem(hadoopConfiguration)
    var cpFileStream: FSDataOutputStream = null
    try {
      // it would be safe to overwrite checkpoint, since we will not start a new job when
      // checkpoint hasn't been committed
      cpFileStream = fs.create(tempProgressTrackingPointPath, true)
      val record = ProgressRecord(recordTime,
                                  uid,
                                  eventHubNameAndPartition.ehName,
                                  eventHubNameAndPartition.partitionId,
                                  cpOffset,
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
