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

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.eventhubscommon.{EventHubNameAndPartition, EventHubsConnector, OffsetRecord}
import org.apache.spark.eventhubscommon.progress.ProgressTrackerBase

/**
 * EventHub uses offset to indicates the startpoint of each receiver, and uses the number of
 * messages for rate control, which are described by offset and sequence number respesctively.
 * As a result, we have to build this class to translate the sequence number to offset for the next
 * batch to start. The basic idea is that the tasks running on executors writes the offset of the
 * last message to HDFS and we gather those files into a progress tracking point for a certain batch
 *
 * @param progressDir the directory of checkpoint files
 * @param appName the name of Spark application
 * @param hadoopConfiguration the hadoop configuration instance
 */
private[spark] class DirectDStreamProgressTracker private[spark](
    progressDir: String,
    appName: String,
    hadoopConfiguration: Configuration)
  extends ProgressTrackerBase(progressDir, appName, hadoopConfiguration) {

  // the lock synchronizing the read and committing operations, since they are executed in driver
  // and listener thread respectively.
  private val driverLock = new Object


  override def eventHubNameAndPartitions: Map[String, List[EventHubNameAndPartition]] = {
    DirectDStreamProgressTracker.registeredConnectors.map {
      connector => (connector.uid, connector.connectedInstances)
    }.toMap
  }

  /**
   * called when ProgressTracker is called for the first time, including recovering from the
   * checkpoint
   */
  override def init(): Unit = {
    // recover from partially executed checkpoint commit
    val fs = progressDirPath.getFileSystem(hadoopConfiguration)
    try {
      val checkpointDirExisted = fs.exists(progressDirPath)
      if (checkpointDirExisted) {
        val (validationPass, latestFile) = validateProgressFile(fs)
        if (!validationPass) {
          if (latestFile.isDefined) {
            logWarning(s"latest progress file ${latestFile.get} corrupt, rolling back...")
            fs.delete(latestFile.get, true)
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

  /**
   * read the progress record for the specified namespace, streamId and timestamp
   */
  override def read(namespace: String, timestamp: Long, fallBack: Boolean):
      OffsetRecord = driverLock.synchronized {
    super.read(namespace, timestamp, fallBack)
  }

  def close(): Unit = {}

  // called in EventHubDirectDStream's clearCheckpointData method
  override def cleanProgressFile(timestampToClean: Long): Unit = driverLock.synchronized {
    val fs = progressDirPath.getFileSystem(hadoopConfiguration)
    // clean progress directory
    // NOTE: due to SPARK-19280 (https://issues.apache.org/jira/browse/SPARK-19280)
    // we have to disable cleanup thread
    /*
    val allUselessFiles = fs.listStatus(progressDirPath, new PathFilter {
      override def accept(path: Path): Boolean = fromPathToTimestamp(path) <= checkpointTime
    }).map(_.getPath)
    val sortedFileList = allUselessFiles.sortWith((p1, p2) => fromPathToTimestamp(p1) >
      fromPathToTimestamp(p2))
    if (sortedFileList.nonEmpty) {
      sortedFileList.tail.foreach { filePath =>
        logInfo(s"delete $filePath")
        fs.delete(filePath, true)
      }
    }
    */
    // clean temp directory
    val allUselessTempFiles = fs.listStatus(progressTempDirPath, new PathFilter {
      override def accept(path: Path): Boolean = fromPathToTimestamp(path) <= timestampToClean
    }).map(_.getPath)
    if (allUselessTempFiles.nonEmpty) {
      allUselessTempFiles.groupBy(fromPathToTimestamp).toList.sortWith((p1, p2) => p1._1 > p2._1).
        tail.flatMap(_._2).foreach {
        filePath => logInfo(s"delete $filePath")
        fs.delete(filePath, true)
      }
    }
  }

  /**
   * commit offsetToCommit to a new progress tracking file
   */
  override def commit(
      offsetToCommit: Map[String, Map[EventHubNameAndPartition, (Long, Long)]],
      commitTime: Long): Unit = driverLock.synchronized {
    super.commit(offsetToCommit, commitTime)
  }
}

object DirectDStreamProgressTracker {

  val registeredConnectors = new ListBuffer[EventHubsConnector]

  private var _progressTracker: DirectDStreamProgressTracker = _

  private[spark] def reset(): Unit = {
    registeredConnectors.clear()
    _progressTracker = null
  }

  def getInstance: ProgressTrackerBase[_ <: EventHubsConnector] = _progressTracker

  private[spark] def initInstance(
      progressDirStr: String,
      appName: String,
      hadoopConfiguration: Configuration): ProgressTrackerBase[_ <: EventHubsConnector] = {
    this.synchronized {
      // DirectDStream shall have singleton progress tracker
      if (_progressTracker == null) {
        _progressTracker = new DirectDStreamProgressTracker(progressDirStr,
          appName,
          hadoopConfiguration)
      }
      _progressTracker.init()
    }
    _progressTracker
  }
}
