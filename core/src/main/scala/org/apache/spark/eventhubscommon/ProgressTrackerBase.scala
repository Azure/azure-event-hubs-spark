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

import java.io.{BufferedReader, InputStreamReader, IOException}

import scala.collection.mutable.ListBuffer

import com.microsoft.azure.eventhubs.PartitionReceiver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.eventhubs.checkpoint.{DirectDStreamProgressTracker, OffsetRecord}

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

  // getModificationTime is not reliable for unit test and some extreme case in distributed
  // file system so that we have to derive timestamp from the file names. The timestamp can be the
  // logical one like batch 1, 2, 3 and can also be the real timestamp
  private[spark] def fromPathToTimestamp(path: Path): Long = {
    path.getName.split("-").last.toLong
  }

  protected def allEventNameAndPartitionExist(
      candidateEhNameAndPartitions: Map[String, List[EventHubNameAndPartition]]): Boolean = {
    eventHubNameAndPartitions.forall{
      case (ehNameSpace, ehNameAndPartitions) =>
        candidateEhNameAndPartitions.contains(ehNameSpace) &&
          ehNameAndPartitions.forall(candidateEhNameAndPartitions(ehNameSpace).contains)
    }
  }

  /**
   * get the latest progress file saved under directory
   */
  protected def getLatestFile(
      directory: Path, fs: FileSystem, timestamp: Long = Long.MaxValue): Option[Path] = {
    require(fs.isDirectory(directory), s"$directory is not a directory")
    val allFiles = fs.listStatus(directory)
    if (allFiles.length < 1) {
      None
    } else {
      Some(allFiles.filter(fsStatus => fromPathToTimestamp(fsStatus.getPath) <= timestamp).
        sortWith((f1, f2) => fromPathToTimestamp(f1.getPath) > fromPathToTimestamp(f2.getPath))
        (0).getPath)
    }
  }

  protected def readProgressRecordLines(
      progressFilePath: Path,
      fs: FileSystem): List[ProgressRecord] = {
    val ret = new ListBuffer[ProgressRecord]
    var ins: FSDataInputStream = null
    var br: BufferedReader = null
    try {
      ins = fs.open(progressFilePath)
      br = new BufferedReader(new InputStreamReader(ins, "UTF-8"))
      var line = br.readLine()
      while (line != null) {
        val progressRecordOpt = ProgressRecord.parse(line)
        if (progressRecordOpt.isEmpty) {
          throw new IllegalStateException(s"detect corrupt progress tracking file at $line" +
            s" it might be a bug in the implementation of underlying file system")
        }
        val progressRecord = progressRecordOpt.get
        ret += progressRecord
        line = br.readLine()
      }
    } catch {
      case ios: IOException =>
        ios.printStackTrace()
        throw ios
    } finally {
      if (br != null) {
        br.close()
      }
    }
    ret.toList
  }

  /**
   * pinpoint the progress file named "progress-timestamp"
   */
  private[spark] def pinPointProgressFile(fs: FileSystem, timestamp: Long): Option[Path] = {
    try {
      require(fs.isDirectory(progressDirPath), s"$progressDirPath is not a directory")
      val targetFilePath = new Path(progressDirPath.toString + s"/progress-$timestamp")
      val targetFileExists = fs.exists(targetFilePath)
      if (targetFileExists) Some(targetFilePath) else None
    } catch {
      case ioe: IOException =>
        logError(ioe.getMessage)
        ioe.printStackTrace()
        throw ioe
      case ias: IllegalArgumentException =>
        logError(ias.getMessage)
        ias.printStackTrace()
        throw ias
      case t: Throwable =>
        logError(s"unknown error ${t.getMessage}")
        t.printStackTrace()
        throw t
    }
  }

  /**
   * read the progress record for the specified progressEntityID and timestamp
   */
  protected def read(targetConnectorID: String, timestamp: Long, fallBack: Boolean,
           predicateWithinProgressFile: (ProgressRecord, String) => Boolean): OffsetRecord = {
    val fs = progressDirPath.getFileSystem(hadoopConfiguration)
    var recordToReturn = Map[EventHubNameAndPartition, (Long, Long)]()
    var readTimestamp: Long = 0
    var progressFileOption: Option[Path] = null
    try {
      progressFileOption = {
        if (!fallBack) {
          pinPointProgressFile(fs, timestamp)
        } else {
          getLatestFile(progressDirPath, fs, timestamp)
        }
      }
      if (progressFileOption.isEmpty) {
        // if no progress file, then start from the beginning of the streams
        val connectedEventHubs = eventHubNameAndPartitions.find {
          case (connectorUID, _) => connectorUID == targetConnectorID}
        require(connectedEventHubs.isDefined, s"cannot find $targetConnectorID in" +
          s" $eventHubNameAndPartitions")
        // it's hacky to take timestamp -1 as the start of streams
        readTimestamp = -1
        recordToReturn = connectedEventHubs.get._2.map(
          (_, (PartitionReceiver.START_OF_STREAM.toLong, -1L))).toMap
      } else {
        val expectedTimestamp = fromPathToTimestamp(progressFileOption.get)
        val progressFilePath = progressFileOption.get
        val recordLines = readProgressRecordLines(progressFilePath, fs)
        require(recordLines.count(_.timestamp != expectedTimestamp) == 0, "detected inconsistent" +
          s" progress record, expected timestamp $expectedTimestamp")
        readTimestamp = expectedTimestamp
        recordToReturn = recordLines.filter(
          progressRecord => predicateWithinProgressFile(progressRecord, targetConnectorID)).map(
          progressRecord => EventHubNameAndPartition(progressRecord.eventHubName,
            progressRecord.partitionId) -> (progressRecord.offset, progressRecord.seqId)).toMap
      }
    } catch {
      case ias: IllegalArgumentException =>
        logError(ias.getMessage)
        ias.printStackTrace()
        throw ias
    }
    OffsetRecord(Time(readTimestamp), recordToReturn)
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