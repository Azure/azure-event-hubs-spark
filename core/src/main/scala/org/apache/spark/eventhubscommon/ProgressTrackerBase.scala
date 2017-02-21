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

package org.apache.spark.eventhubscommon

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.eventhubs.checkpoint.DirectDStreamProgressTracker

private[spark] abstract class ProgressTrackerBase[T <: EventHubsConnector](
    progressDir: String, appName: String, hadoopConfiguration: Configuration) extends Logging {


  protected val progressDirStr: String = PathTools.progressDirPathStr(progressDir, appName)
  protected val progressTempDirStr: String = PathTools.progressTempDirPathStr(progressDir,
    appName)

  private[spark] val progressDirPath = new Path(progressDirStr)
  private[spark] val progressTempDirPath = new Path(progressTempDirStr)

  def eventHubNameAndPartitions: Map[String, List[EventHubNameAndPartition]] = {
    ProgressTrackerBase.registeredConnectors.map {
      connector => (connector.uid, connector.connectedInstances)
    }.toMap
  }

  def init()
}


private[spark] object ProgressTrackerBase {
  val registeredConnectors = new ListBuffer[EventHubsConnector]

  private var _progressTracker: ProgressTrackerBase[_ <: EventHubsConnector] = _

  private[spark] def reset(): Unit = {
    registeredConnectors.clear()
    _progressTracker = null
  }

  def getInstance: ProgressTrackerBase[_ <: EventHubsConnector] = _progressTracker

  private[spark] def initInstance(
      progressDirStr: String,
      appName: String,
      hadoopConfiguration: Configuration,
      ProgressTrackerType: String): ProgressTrackerBase[_ <: EventHubsConnector] =
    this.synchronized {
      if (_progressTracker == null) {
        ProgressTrackerType match {
          case "directDStream" =>
            _progressTracker = new DirectDStreamProgressTracker(progressDirStr,
              appName,
              hadoopConfiguration)
        }
        _progressTracker.init()
      }
    _progressTracker
  }
}