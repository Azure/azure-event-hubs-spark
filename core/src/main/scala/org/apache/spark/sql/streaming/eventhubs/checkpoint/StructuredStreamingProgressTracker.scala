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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.eventhubs.common.{ NameAndPartition, EventHubsConnector }
import org.apache.spark.eventhubs.common.EventHubsConnector
import org.apache.spark.eventhubs.common.progress.{ PathTools, ProgressTrackerBase }

private[spark] class StructuredStreamingProgressTracker private[spark] (
    uid: String,
    progressDir: String,
    appName: String,
    hadoopConfiguration: Configuration)
    extends ProgressTrackerBase(progressDir, appName, hadoopConfiguration) {

  private[spark] override lazy val progressDirectoryStr =
    PathTools.makeProgressDirectoryStr(progressDir, appName, uid)
  private[spark] override lazy val tempDirectoryStr =
    PathTools.makeTempDirectoryStr(progressDir, appName, uid)
  private[spark] override lazy val metadataDirectoryStr =
    PathTools.makeMetadataDirectoryStr(progressDir, appName, uid)

  override def eventHubNameAndPartitions: Map[String, List[NameAndPartition]] = {
    val connector = StructuredStreamingProgressTracker.registeredConnectors(uid)
    Map(connector.uid -> connector.namesAndPartitions)
  }

  private def initMetadataDirectory(): Unit = {
    try {
      val fs = metadataDirectoryPath.getFileSystem(hadoopConfiguration)
      val checkpointMetadaDirExisted = fs.exists(tempDirectoryPath)
      if (!checkpointMetadaDirExisted) {
        fs.mkdirs(metadataDirectoryPath)
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
  }

  private def initProgressFileDirectory(): Unit = {
    val fs = progressDirectoryPath.getFileSystem(hadoopConfiguration)
    try {
      val progressDirExist = fs.exists(progressDirectoryPath)
      if (progressDirExist) {
        val (validationPass, latestFile) = validateProgressFile(fs)
        if (!validationPass && latestFile.isDefined) {
          logWarning(s"latest progress file ${latestFile.get} corrupt, rebuild file...")
          val latestFileTimestamp = fromPathToTimestamp(latestFile.get)
          val progressRecords = collectProgressRecordsForBatch(
            latestFileTimestamp,
            List(StructuredStreamingProgressTracker.registeredConnectors(uid)))
          commit(progressRecords, latestFileTimestamp)
        }
      } else {
        fs.mkdirs(progressDirectoryPath)
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
  }

  override def init(): Unit = {
    initProgressFileDirectory()
    initMetadataDirectory()
  }
}

object StructuredStreamingProgressTracker {

  val registeredConnectors = new mutable.HashMap[String, EventHubsConnector]

  private var _progressTrackers = new mutable.HashMap[String, StructuredStreamingProgressTracker]

  private[spark] def reset(): Unit = {
    registeredConnectors.clear()
    _progressTrackers.values.map(pt => pt.metadataCleanupFuture.cancel(true))
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
        _progressTrackers += uid -> new StructuredStreamingProgressTracker(uid,
                                                                           progressDirStr,
                                                                           appName,
                                                                           hadoopConfiguration)
      }
      _progressTrackers(uid).init()
    }
    _progressTrackers(uid)
  }
}
