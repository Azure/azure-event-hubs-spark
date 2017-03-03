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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.eventhubscommon.{EventHubNameAndPartition, EventHubsConnector}
import org.apache.spark.eventhubscommon.progress.{PathTools, ProgressTrackerBase}

class StructuredStreamingProgressTracker(
    uid: String,
    progressDir: String,
    appName: String,
    hadoopConfiguration: Configuration)
  extends ProgressTrackerBase(progressDir, appName, hadoopConfiguration) {

  protected override lazy val progressDirStr: String = PathTools.progressDirPathStr(
    progressDir, appName, uid)
  protected override lazy val progressTempDirStr: String = PathTools.progressTempDirPathStr(
    progressDir, appName, uid)

  override def eventHubNameAndPartitions: Map[String, List[EventHubNameAndPartition]] = {
    val connector = StructuredStreamingProgressTracker.registeredConnectors(uid)
    Map(connector.uid -> connector.connectedInstances)
  }

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
            commit(progressRecords, latestFileTimestamp)
          }
        }
      } else {
        fs.mkdirs(progressDirPath)
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    } finally {
      // EMPTY
    }
  }
}

object StructuredStreamingProgressTracker {

  val registeredConnectors = new mutable.HashMap[String, EventHubsConnector]

  private var _progressTrackers = new mutable.HashMap[String, StructuredStreamingProgressTracker]

  private[spark] def reset(): Unit = {
    registeredConnectors.clear()
    _progressTrackers.clear()
  }

  def getInstance(uid: String): ProgressTrackerBase[_ <: EventHubsConnector] =
    _progressTrackers(uid)

  private[spark] def initInstance(
      uid: String,
      progressDirStr: String,
      appName: String,
      hadoopConfiguration: Configuration): ProgressTrackerBase[_ <: EventHubsConnector] = {
    this.synchronized {
      // DirectDStream shall have singleton progress tracker
      if (_progressTrackers.get(uid).isEmpty) {
        _progressTrackers += uid -> new StructuredStreamingProgressTracker(uid, progressDirStr,
          appName,
          hadoopConfiguration)
      }
      _progressTrackers(uid).init()
    }
    _progressTrackers(uid)
  }
}
