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

package org.apache.spark.eventhubscommon.progress

import org.apache.spark.eventhubscommon.EventHubNameAndPartition

private[spark] object PathTools extends Serializable {

  private def fromSubDirNamesToString(subDirs: Seq[String]): String = {
    subDirs.mkString("/")
  }

  def progressDirPathStr(checkpointDir: String, subDirNames: String*): String = {
    s"$checkpointDir/${fromSubDirNamesToString(subDirNames)}"
  }

  def progressTempDirPathStr(checkpointDir: String, subDirNames: String*): String = {
    s"$checkpointDir/${fromSubDirNamesToString(subDirNames)}_temp"
  }

  def progressTempFileNamePattern(
      streamId: Int,
      uid: String,
      eventHubNameAndPartition: EventHubNameAndPartition,
      timestamp: Long): String = {
    s"$streamId-$uid-$eventHubNameAndPartition-$timestamp"
  }

  def progressFileNamePattern(timestamp: Long): String = {
    s"progress-$timestamp"
  }

  def progressTempFileStr(
      basePath: String,
      streamId: Int,
      uid: String,
      eventHubNameAndPartition: EventHubNameAndPartition,
      timestamp: Long): String = {
    basePath + "/" + progressTempFileNamePattern(streamId, uid, eventHubNameAndPartition, timestamp)
  }
}
