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

package org.apache.spark.eventhubscommon.checkpoint

import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.eventhubscommon.progress.{PathTools, ProgressRecord}

private[spark] object ProgressTrackingCommon {
  def writeProgressFile(
      progressPath: String,
      streamId: Int,
      fs: FileSystem,
      timestamp: Long,
      namespace: String,
      ehName: String,
      partitionRange: Range,
      offset: Int,
      seq: Int): Unit = {
    for (partitionId <- partitionRange) {
      Files.write(
        Paths.get(progressPath + s"/${PathTools.progressFileNamePattern(timestamp)}"),
        (ProgressRecord(timestamp, namespace, ehName, partitionId, offset,
          seq).toString + "\n").getBytes, {
          if (Files.exists(Paths.get(progressPath +
            s"/${PathTools.progressFileNamePattern(timestamp)}"))) {
            StandardOpenOption.APPEND
          } else {
            StandardOpenOption.CREATE
          }
        })
    }
  }

  def createMetadataFile(fs: FileSystem, metadataPath: String, timestamp: Long): Unit = {
    fs.create(new Path(s"$metadataPath/${PathTools.progressMetadataNamePattern(timestamp)}"))
  }
}
