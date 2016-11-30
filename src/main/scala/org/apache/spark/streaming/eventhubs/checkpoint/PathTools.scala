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

private[checkpoint] object PathTools extends Serializable {

  def checkpointDirPathStr(checkpointDir: String, appName: String, streamId: Int,
                           eventHubNamespace: String): String = {
    s"$checkpointDir/$appName/$streamId/$eventHubNamespace"
  }

  def tempCheckpointDirStr(checkpointDir: String, appName: String,
                           streamId: Int,
                           eventHubNamespace: String): String = {
    s"${checkpointDirPathStr(checkpointDir, appName, streamId, eventHubNamespace)}_temp"
  }

  def checkpointBackupDirStr(checkpointDir: String, appName: String,
                             streamId: Int): String = {
    s"${checkpointDir}_backup/$appName/$streamId/"
  }

  def checkpointFilePathStr(checkpointDir: String, appName: String, streamId: Int,
                            eventHubNamespace: String,
                            eventHubName: String, partitionId: Int): String = {
    val tempDir = tempCheckpointDirStr(checkpointDir, appName, streamId, eventHubNamespace)
    s"$tempDir/$eventHubName-partition-$partitionId"
  }
}
