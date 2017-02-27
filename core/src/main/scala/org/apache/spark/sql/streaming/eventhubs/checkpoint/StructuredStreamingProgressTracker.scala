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

package org.apache.spark.sql.streaming.eventhubs.checkpoint

import java.io.{BufferedReader, IOException, InputStreamReader}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.progress.{ProgressRecord, ProgressTrackerBase}

class StructuredStreamingProgressTracker(
    progressDir: String,
    appName: String,
    hadoopConfiguration: Configuration)
  extends ProgressTrackerBase(progressDir, appName, hadoopConfiguration) {

  override def init(): Unit = {
    // recover from partially executed checkpoint commit
    val fs = progressDirPath.getFileSystem(hadoopConfiguration)
    try {
      val checkpointDirExisted = fs.exists(progressDirPath)
      if (checkpointDirExisted) {
        val (validationPass, latestFile) = validateProgressFile(fs)
        if (!validationPass) {
          if (latestFile.isDefined) {
            logWarning(s"latest progress file ${latestFile.get} corrupt, rebuild file...")
            val latestFileTimestamp = fromPathToTimestamp(latestFile.get)
            val progressRecords = collectProgressRecordsForBatch(latestFileTimestamp)
            // commit()
          }
        }
      } else {
        fs.mkdirs(progressDirPath)
      }
      val checkpointTempDirExisted = fs.exists(progressTempDirPath)
      if (checkpointTempDirExisted) {
        fs.delete(progressTempDirPath, true)
        logInfo(s"cleanup temp checkpoint $progressTempDirPath")
      }
      fs.mkdirs(progressTempDirPath)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    } finally {
      // EMPTY
    }
  }
}
