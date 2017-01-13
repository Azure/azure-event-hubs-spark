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
 * @param progressDir the directory of checkpoint files
 * @param appName the name of Spark application
 * @param hadoopConfiguration the hadoop configuration instance
 */
private[eventhubs] class ProgressTracker private[checkpoint](
    progressDir: String,
    appName: String,
    private val hadoopConfiguration: Configuration) extends Logging {

  private val progressDirStr = PathTools.progressDirPathStr(progressDir, appName)
  private val progressTempDirStr = PathTools.progressTempDirPathStr(progressDir,
    appName)

  private[eventhubs] val progressDirPath = new Path(progressDirStr)
  private[eventhubs] val progressTempDirPath = new Path(progressTempDirStr)

  // the lock synchronizing the read and committing operations, since they are executed in driver
  // and listener thread respectively.
  private val driverLock = new Object

  def eventHubNameAndPartitions: Map[String, List[EventHubNameAndPartition]] = {
    ProgressTracker.eventHubDirectDStreams.map { directStream =>
      val namespace = directStream.eventHubNameSpace
      val ehSpace = directStream.eventhubNameAndPartitions
      (namespace, ehSpace.toList)
    }.toMap
  }

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
      Some(allFiles.sortWith((f1, f2) =>
        fromPathToTimestamp(f1.getPath) > fromPathToTimestamp(f2.getPath))(0).getPath)
    }
  }

  // getModificationTime is not reliable for unit test and some extreme case in distributed
  // file system so that we have to derive timestamp from the file names
  private[eventhubs] def fromPathToTimestamp(path: Path): Long = {
    path.getName.split("-").last.toLong
  }

  /**
   * this method is called when ProgressTracker is started for the first time (including recovering
   * from checkpoint). This method validate the latest progress file by checking whether it
   * contains progress of all partitions we subscribe to. If not, we will delete the corrupt
   * progress file
   * @return (whether the latest file pass the validation, whether the latest file exists)
   *          (false, Some(x)) does not exist
   */
  private def validateProgressFile(fs: FileSystem): (Boolean, Option[Path]) = {
    val latestFileOpt = getLastestFile(progressDirPath, fs)
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

  /**
   * locate the progress file according to the timestamp
   * when the timestamp of the latest file in the progress tracking directory is earlier than
   * the passed-in timestamp we return the latest file; otherwise we return the file which is
   * committed at batch (timestamp - batchDuratiuon)
   */
  private[checkpoint] def locateProgressFile(fs: FileSystem, timestamp: Long): Option[Path] = {
    try {
      val latestFilePathOpt = getLastestFile(progressDirPath, fs)
      if (latestFilePathOpt.isDefined) {
        val latestFile = latestFilePathOpt.get
        val latestTimestamp = fromPathToTimestamp(latestFile)
        if (latestTimestamp < timestamp) {
          latestFilePathOpt
        } else {
          val allFiles = fs.listStatus(progressDirPath)
          val sortedFiles = allFiles.filter(fileStatus =>
            fromPathToTimestamp(fileStatus.getPath) < timestamp).
            sortWith((f1, f2) =>
              fromPathToTimestamp(f1.getPath) > fromPathToTimestamp(f2.getPath))
          if (sortedFiles.length < 1) {
            None
          } else {
            Some(sortedFiles.head.getPath)
          }
        }
      } else {
        latestFilePathOpt
      }
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

  private def readProgressRecordLines(
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
   * read the progress record for the specified namespace, streamId and timestamp
   */
  def read(namespace: String, timestamp: Long):
      Map[EventHubNameAndPartition, (Long, Long)] = driverLock.synchronized {
    val fs = progressDirPath.getFileSystem(hadoopConfiguration)
    var ret = Map[EventHubNameAndPartition, (Long, Long)]()
    var progressFileOption: Option[Path] = null
    try {
      progressFileOption = locateProgressFile(fs, timestamp)
      if (progressFileOption.isEmpty) {
        // if no progress file, then start from the beginning of the streams
        val namespaceToEventHubs = eventHubNameAndPartitions.find {
          case (ehNamespace, _) => ehNamespace == namespace}
        require(namespaceToEventHubs.isDefined, s"cannot find $namespace in" +
          s" $eventHubNameAndPartitions")
        ret = namespaceToEventHubs.get._2.map((_, (PartitionReceiver.START_OF_STREAM.toLong, -1L))).
          toMap
      } else {
        val expectedTimestamp = fromPathToTimestamp(progressFileOption.get)
        val progressFilePath = progressFileOption.get
        val recordLines = readProgressRecordLines(progressFilePath, fs)
        require(recordLines.count(_.timestamp != expectedTimestamp) == 0, "detected inconsistent" +
          s" progress record, expected timestamp $expectedTimestamp")
        ret = recordLines.filter(progressRecord => progressRecord.namespace == namespace).
          map(progressRecord => EventHubNameAndPartition(progressRecord.eventHubName,
            progressRecord.partitionId) -> (progressRecord.offset, progressRecord.seqId)).toMap
      }
    } catch {
      case ias: IllegalArgumentException =>
        logError(ias.getMessage)
        ias.printStackTrace()
        throw ias
    }
    ret
  }

  def close(): Unit = {}

  // write offsetToCommit to a progress tracking file
  private def transaction(
      offsetToCommit: Map[(String, Int), Map[EventHubNameAndPartition, (Long, Long)]],
      fs: FileSystem,
      commitTime: Long): Unit = {
    var oos: FSDataOutputStream = null
    try {
      oos = fs.create(new Path(progressDirStr + s"/progress-$commitTime"))
      offsetToCommit.foreach {
        case ((namespace, streamId), ehNameAndPartitionToOffsetAndSeq) =>
          ehNameAndPartitionToOffsetAndSeq.foreach {
            case (nameAndPartitionId, (offset, seq)) =>
              oos.writeBytes(
                ProgressRecord(commitTime, namespace, streamId,
                  nameAndPartitionId.eventHubName, nameAndPartitionId.partitionId, offset,
                  seq).toString + "\n"
              )
          }
      }
    } finally {
      if (oos != null) {
        oos.close()
      }
    }
  }

  // called in EventHubDirectDStream's clearCheckpointData method
  def cleanProgressFile(checkpointTime: Long): Unit = driverLock.synchronized {
    // because offset committing and checkpoint data cleanup are performed in two different threads,
    // we need to always keep the latest file instead of blindly delete all files earlier than
    // checkpointTime.
    val fs = new Path(progressDir).getFileSystem(hadoopConfiguration)
    // clean progress directory
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
  def commit(offsetToCommit: Map[(String, Int), Map[EventHubNameAndPartition, (Long, Long)]],
      commitTime: Long): Unit = driverLock.synchronized {
    val fs = new Path(progressDir).getFileSystem(hadoopConfiguration)
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

  private def allProgressRecords(timestamp: Long): List[FileStatus] = {
    val fs = progressTempDirPath.getFileSystem(hadoopConfiguration)
    val r = fs.listStatus(progressTempDirPath, new PathFilter {
      override def accept(path: Path) = {
        path.getName.split("-").last == timestamp.toString
      }
    }).toList
    r
  }

  /**
   * read progress records from temp directories
   * @return Map(Namespace -> Map(EventHubNameAndPartition -> (Offset, Seq))
   */
  def collectProgressRecordsForBatch(timestamp: Long):
      Map[String, Map[EventHubNameAndPartition, (Long, Long)]] = {
    val records = new ListBuffer[ProgressRecord]
    val ret = new mutable.HashMap[String, Map[EventHubNameAndPartition, (Long, Long)]]
    try {
      val fs = progressTempDirPath.getFileSystem(new Configuration())
      val files = allProgressRecords(timestamp).iterator
      logInfo(s"$files")
      while (files.hasNext) {
        val file = files.next()
        val progressRecords = readProgressRecordLines(file.getPath, fs)
        records ++= progressRecords
      }
      // check timestamp consistency
      records.foreach(progressRecord =>
        if (timestamp != progressRecord.timestamp) {
            throw new IllegalStateException(s"detect inconsistent progress tracking file at" +
              s" $progressRecord, expected timestamp: $timestamp, it might be a bug in the" +
              s" implementation of underlying file system")
        })
    } catch {
      case ioe: IOException =>
        logError(s"error: ${ioe.getMessage}")
        ioe.printStackTrace()
        throw ioe
      case t: Throwable =>
        logError(s"unknown error ${t.getMessage}")
        t.printStackTrace()
        throw t
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

  private[eventhubs] val eventHubDirectDStreams = new ListBuffer[EventHubDirectDStream]

  private var _progressTracker: ProgressTracker = _

  private[eventhubs] def reset(): Unit = {
    eventHubDirectDStreams.clear()
    _progressTracker = null
  }

  def getInstance: ProgressTracker = _progressTracker

  private[eventhubs] def initInstance(
      progressDirStr: String,
      appName: String,
      hadoopConfiguration: Configuration): ProgressTracker = this.synchronized {
    if (_progressTracker == null) {
      _progressTracker = new ProgressTracker(progressDirStr,
        appName,
        hadoopConfiguration)
      _progressTracker.init()
    }
    _progressTracker
  }
}
