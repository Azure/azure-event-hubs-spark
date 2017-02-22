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

import java.io.{BufferedReader, InputStreamReader, IOException}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.PartitionReceiver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.eventhubscommon.{EventHubNameAndPartition, ProgressRecord, ProgressTrackerBase}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Time


case class OffsetRecord(timestamp: Time, offsets: Map[EventHubNameAndPartition, (Long, Long)])

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
  extends ProgressTrackerBase(progressDir, appName, hadoopConfiguration) with Logging {

  // the lock synchronizing the read and committing operations, since they are executed in driver
  // and listener thread respectively.
  private val driverLock = new Object

  /**
   * this method is called when ProgressTracker is started for the first time (including recovering
   * from checkpoint). This method validate the latest progress file by checking whether it
   * contains progress of all partitions we subscribe to. If not, we will delete the corrupt
   * progress file
   * @return (whether the latest file pass the validation, whether the latest file exists)
   *          (false, Some(x)) does not exist
   */
  private def validateProgressFile(fs: FileSystem): (Boolean, Option[Path]) = {
    val latestFileOpt = getLatestFile(progressDirPath, fs)
    val allProgressFiles = new mutable.HashMap[String, List[EventHubNameAndPartition]]
    var br: BufferedReader = null
    try {
      if (latestFileOpt.isEmpty) {
        return (false, None)
      }
      val cpFile = fs.open(latestFileOpt.get)
      br = new BufferedReader(new InputStreamReader(cpFile, "UTF-8"))
      var cpRecord: String = br.readLine()
      var timestamp = -1L
      while (cpRecord != null) {
        val progressRecordOpt = ProgressRecord.parse(cpRecord)
        if (progressRecordOpt.isEmpty) {
          return (false, latestFileOpt)
        }
        val progressRecord = progressRecordOpt.get
        val newList = allProgressFiles.getOrElseUpdate(progressRecord.namespace,
          List[EventHubNameAndPartition]()) :+
          EventHubNameAndPartition(progressRecord.eventHubName, progressRecord.partitionId)
        allProgressFiles(progressRecord.namespace) = newList
        if (timestamp == -1L) {
          timestamp = progressRecord.timestamp
        } else if (progressRecord.timestamp != timestamp) {
          return (false, latestFileOpt)
        }
        cpRecord = br.readLine()
      }
      br.close()
    } catch {
      case ios: IOException =>
        throw ios
      case t: Throwable =>
        t.printStackTrace()
        return (false, latestFileOpt)
    } finally {
      if (br != null) {
        br.close()
      }
    }
    (allEventNameAndPartitionExist(allProgressFiles.toMap), latestFileOpt)
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
  def read(namespace: String, timestamp: Long, fallBack: Boolean):
      OffsetRecord = driverLock.synchronized {
    read(namespace, timestamp, fallBack,
      (pr: ProgressRecord, expectedNamespace: String) => pr.namespace == expectedNamespace)
  }

  def close(): Unit = {}

  // called in EventHubDirectDStream's clearCheckpointData method
  def cleanProgressFile(checkpointTime: Long): Unit = driverLock.synchronized {
    // because offset committing and checkpoint data cleanup are performed in two different threads,
    // we need to always keep the latest file instead of blindly delete all files earlier than
    // checkpointTime.
    val fs = new Path(progressDir).getFileSystem(hadoopConfiguration)
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
      override def accept(path: Path): Boolean = fromPathToTimestamp(path) <= checkpointTime
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
      offsetToCommit: Map[(String, Int), Map[EventHubNameAndPartition, (Long, Long)]],
      commitTime: Long): Unit = driverLock.synchronized {
    super.commit(offsetToCommit, commitTime)
  }


}
