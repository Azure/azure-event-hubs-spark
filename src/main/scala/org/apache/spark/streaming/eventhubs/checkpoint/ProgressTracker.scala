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
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.eventhubs.{EventHubDirectDStream, EventHubNameAndPartition}

/**
 * EventHub uses offset to indicates the startpoint of each receiver, and uses the number of
 * messages for rate control, which are described by offset and sequence number respesctively.
 * As a result, we have to build this class to translate the sequence number to offset for the next
 * batch to start. The basic idea is that the tasks running on executors writes the offset of the
 * last message to HDFS and we gather those files into a progress tracking point for a certain batch
 *
 * @param checkpointDir the directory of checkpoint files
 * @param appName the name of Spark application
 * @param hadoopConfiguration the hadoop configuration instance
 * @param eventHubNameAndPartitions the list of EventHubNameAndPartition instances which this
 *                                  progress tracker take care of, this parameter is used to verify
 *                                  whether the progress tracker is broken
 */
private[eventhubs] class ProgressTracker private[checkpoint](
    checkpointDir: String,
    appName: String,
    private val hadoopConfiguration: Configuration,
    private[eventhubs] val eventHubNameAndPartitions: Map[String, List[EventHubNameAndPartition]])
  extends Logging {

  private val progressDirStr = PathTools.progressDirPathStr(checkpointDir, appName)
  private val progressTempDirStr = PathTools.progressTempDirPathStr(checkpointDir,
    appName)

  private[eventhubs] val progressDirPath = new Path(progressDirStr)
  private[eventhubs] val progressTempDirPath = new Path(progressTempDirStr)

  // the lock synchronizing the read and committing operations, since they are executed in driver
  // and listener thread respectively.
  private val driverLock = new Object


  private def allEventNameAndPartitionExist(
    candidateEhNameAndPartitions: Map[String, List[EventHubNameAndPartition]]): Boolean = {
    eventHubNameAndPartitions.forall{
      case (ehNameSpace, ehNameAndPartitions) =>
        candidateEhNameAndPartitions.contains(ehNameSpace) &&
          ehNameAndPartitions.forall(candidateEhNameAndPartitions(ehNameSpace).contains)
    }
  }

  private def getLastestFile(directory: Path, fs: FileSystem): Option[Path] = {
    require(fs.isDirectory(directory), s"$directory is not a directory")
    val allFiles = fs.listStatus(directory)
    if (allFiles.length < 1) {
      None
    } else {
      // getModificationTime is not reliable for unit test and some extreme case in file system
      Some(allFiles.sortWith((f1, f2) =>
        f1.getPath.getName.split("-").last.toLong > f2.getPath.getName.split("-").last.toLong)(0).
        getPath)
    }
  }

  private def fromPathToTimestamp(path: Path): Long = {
    path.getName.split("-").last.toLong
  }

  private def validateProgressFile(fs: FileSystem): (Boolean, Option[Path]) = {
    val latestFileOpt = getLastestFile(progressDirPath, fs)
    val allCheckpointedNameAndPartitions = new mutable.HashMap[String,
      List[EventHubNameAndPartition]]
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
        val progressRecord = ProgressRecord.parse(cpRecord)
        val newList = allCheckpointedNameAndPartitions.getOrElseUpdate(progressRecord.namespace,
          List[EventHubNameAndPartition]()) :+
          EventHubNameAndPartition(progressRecord.eventHubName, progressRecord.partitionId)
        allCheckpointedNameAndPartitions(progressRecord.namespace) = newList
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
    (allEventNameAndPartitionExist(allCheckpointedNameAndPartitions.toMap), latestFileOpt)
  }

  private def init(): Unit = {
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

  private def locateProgressFile(fs: FileSystem, timestamp: Long): Option[Path] = {
    val latestFilePathOpt = getLastestFile(progressDirPath, fs)
    if (latestFilePathOpt.isDefined) {
      val latestFile = latestFilePathOpt.get
      val latestTimestamp = fromPathToTimestamp(latestFile)
      if (latestTimestamp < timestamp) {
        latestFilePathOpt
      } else {
        val allFiles = fs.listStatus(progressDirPath)
        Some(
          allFiles.filter(fileStatus => fromPathToTimestamp(fileStatus.getPath) < timestamp).
            sortWith((f1, f2) => fromPathToTimestamp(f1.getPath) > fromPathToTimestamp(f2.getPath)).
            head.getPath
        )
      }
    } else {
      latestFilePathOpt
    }
  }

  def read(namespace: String, streamId: Int, timestamp: Long):
      Map[EventHubNameAndPartition, (Long, Long)] = driverLock.synchronized {
    val fs = progressDirPath.getFileSystem(hadoopConfiguration)
    var ins: FSDataInputStream = null
    var br: BufferedReader = null
    var ret = Map[EventHubNameAndPartition, (Long, Long)]()
    var progressFileOption: Option[Path] = null
    try {
      progressFileOption = locateProgressFile(fs, timestamp)
      if (progressFileOption.isEmpty) {
        // if no progress file, then start from the beginning of the streams
        val namespaceToEventHubs = eventHubNameAndPartitions.find {
          case (ehNamespace, ehList) => ehNamespace == namespace}
        require(namespaceToEventHubs.isDefined, s"cannot find $namespace in" +
          s" $eventHubNameAndPartitions")
        ret = namespaceToEventHubs.get._2.map((_, (PartitionReceiver.START_OF_STREAM.toLong, -1L))).
          toMap
      } else {
        val expectedTimestamp = fromPathToTimestamp(progressFileOption.get)
        val progressFilePath = progressFileOption.get
        ins = fs.open(progressFilePath)
        br = new BufferedReader(new InputStreamReader(ins, "UTF-8"))
        var line = br.readLine()
        while (line != null) {
          val progressRecord = ProgressRecord.parse(line)
          if (progressRecord.timestamp != expectedTimestamp) {
            throw new IllegalStateException(s"detect inconsistent checkpoint at $line, expected" +
              s" timestamp: $timestamp, it might be a bug in the implementation of" +
              s" underlying file system")
          }
          if (progressRecord.streamId == streamId) {
            ret +=
              EventHubNameAndPartition(progressRecord.eventHubName, progressRecord.partitionId) ->
                (progressRecord.offset, progressRecord.seqId)
          }
          line = br.readLine()
        }
      }
    } catch {
      case ioe: IOException =>
        logError(ioe.getMessage)
        ioe.printStackTrace()
        throw ioe
      case ils: IllegalStateException =>
        logError(ils.getMessage)
        ils.printStackTrace()
        throw ils
      case t: Throwable =>
        logError(s"unknown error ${t.getMessage}")
        t.printStackTrace()
        throw t
    } finally {
      if (br != null) {
        br.close()
      }
    }
    ret
  }

  def close(): Unit = {}

  private def transaction(
      offsetToCommit: Map[(String, Int), Map[EventHubNameAndPartition, (Long, Long)]],
      fs: FileSystem,
      time: Long): Unit = {
    var oos: FSDataOutputStream = null
    try {
      oos = fs.create(new Path(progressDirStr + s"/progress-$time"))
      offsetToCommit.foreach {
        case ((namespace, streamId), ehNameAndPartitionToOffsetAndSeq) =>
          ehNameAndPartitionToOffsetAndSeq.foreach {
            case (nameAndPartitionId, (offset, seq)) =>
              oos.writeBytes(
                ProgressRecord(time, namespace, streamId,
                  nameAndPartitionId.eventHubName, nameAndPartitionId.partitionId, offset,
                  seq).toString + "\n"
              )
          }
      }
      fs.delete(progressTempDirPath, true)
      fs.mkdirs(progressTempDirPath)
    } catch {
      case ioe: IOException =>
        ioe.printStackTrace()
        throw ioe
    } finally {
      if (oos != null) {
        oos.close()
      }
    }
  }

  def commit(offsetToCommit: Map[(String, Int), Map[EventHubNameAndPartition, (Long, Long)]],
      commitTime: Long): Unit = driverLock.synchronized {
    val fs = new Path(checkpointDir).getFileSystem(hadoopConfiguration)
    try {
      transaction(offsetToCommit, fs, commitTime)
    } catch {
      case ioe: IOException =>
        ioe.printStackTrace()
        throw ioe
    } finally {
      // EMPTY, we leave the cleanup of partially executed transaction to the moment when recovering
      // from failure
    }
  }

  def snapshot(): Map[String, Map[EventHubNameAndPartition, (Long, Long)]] = {
    val fs = progressTempDirPath.getFileSystem(hadoopConfiguration)
    val records = new ListBuffer[ProgressRecord]
    val ret = new mutable.HashMap[String, Map[EventHubNameAndPartition, (Long, Long)]]
    var br: BufferedReader = null
    // read timestamp
    val files = fs.listFiles(progressTempDirPath, false)
    try {
      var timestamp = -1L
      while (files.hasNext) {
        val file = files.next()
        val inputStream = fs.open(file.getPath)
        br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
        val line = br.readLine()
        if (line == null) {
          throw new IOException(s"cannot read progress file in ${file.getPath}")
        }
        val progressRecord = ProgressRecord.parse(line)
        if (timestamp == -1L) {
          timestamp = progressRecord.timestamp
        } else {
          if (progressRecord.timestamp != timestamp) {
            throw new IllegalStateException(s"detect inconsistent checkpoint at $line," +
              s" expected timestamp: $timestamp, it might be a bug in the implementation of" +
              s" underlying file system")
          }
        }
        records += progressRecord
        br.close()
      }
    } catch {
      case ioe: IOException =>
        logError(s"error: ${ioe.getMessage}")
        ioe.printStackTrace()
        throw ioe
      case ise: IllegalStateException =>
        logError(s"error ${ise.getMessage}")
        ise.printStackTrace()
        throw ise
      case t: Throwable =>
        logError(s"unknown error ${t.getMessage}")
        t.printStackTrace()
        throw t
    } finally {
      if (br != null) {
        br.close()
      }
    }
    // produce the return value
    records.foreach { progressRecord =>
      val newMap = ret.getOrElseUpdate(progressRecord.namespace, Map()) +
        (EventHubNameAndPartition(progressRecord.eventHubName, progressRecord.partitionId) ->
          (progressRecord.offset, progressRecord.seqId))
      ret(progressRecord.namespace) = newMap
    }
    ret.toMap
  }
}

private[eventhubs] object ProgressTracker {

  private var _progressTracker: ProgressTracker = _

  private[checkpoint] def reset(): Unit = {
    _progressTracker = null
  }

  def getInstance(
      ssc: StreamingContext,
      progressDirStr: String,
      appName: String,
      hadoopConfiguration: Configuration): ProgressTracker = this.synchronized {
    if (_progressTracker == null) {
      _progressTracker = new ProgressTracker(progressDirStr,
        appName,
        hadoopConfiguration,
        ProgressTrackingListener.eventHubDirectDStreams.map{directStream =>
          val namespace = directStream.eventHubNameSpace
          val ehSpace = directStream.eventhubNameAndPartitions
          (namespace, ehSpace.toList)
        }.toMap)
      _progressTracker.init()
    }
    _progressTracker
  }
}
