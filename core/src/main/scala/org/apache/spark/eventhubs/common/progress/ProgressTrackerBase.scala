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

package org.apache.spark.eventhubs.common.progress

import java.io.{ BufferedReader, IOException, InputStreamReader }
import java.util.concurrent.{ ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.microsoft.azure.eventhubs.PartitionReceiver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.eventhubs.common.{ NameAndPartition, EventHubsConnector, OffsetRecord }
import org.apache.spark.internal.Logging

private[spark] abstract class ProgressTrackerBase[T <: EventHubsConnector](
    progressDir: String,
    appName: String,
    hadoopConfiguration: Configuration)
    extends Logging {

  private[spark] lazy val progressDirectoryStr =
    PathTools.makeProgressDirectoryStr(progressDir, appName)
  private[spark] lazy val tempDirectoryStr = PathTools.makeTempDirectoryStr(progressDir, appName)
  private[spark] lazy val metadataDirectoryStr =
    PathTools.makeMetadataDirectoryStr(progressDir, appName)

  private[spark] lazy val progressDirectoryPath = new Path(progressDirectoryStr)
  private[spark] lazy val tempDirectoryPath = new Path(tempDirectoryStr)
  private[spark] lazy val metadataDirectoryPath = new Path(metadataDirectoryStr)

  def eventHubNameAndPartitions: Map[String, List[NameAndPartition]]

  // Metadata is maintained for fast ProgressTracker initialization and can cleaned independently
  // from Spark checkpoint files. We'll clean metadata here to ensure it's cleaned whether or not
  // Spark checkpointing is enabled.
  private val threadPoolForMetadataClean = new ScheduledThreadPoolExecutor(1)
  protected val metadataCleanupFuture: ScheduledFuture[_] = scheduleMetadataCleanTask()

  // getModificationTime is not reliable for unit test and some extreme case in distributed
  // file system so that we have to derive timestamp from the file names. The timestamp can be the
  // logical one like batch 1, 2, 3 and can also be the real timestamp
  private[spark] def fromPathToTimestamp(path: Path): Long =
    path.getName.split("-").last.toLong

  protected def allEventNameAndPartitionExist(
      candidateEhNameAndPartitions: Map[String, List[NameAndPartition]]): Boolean = {
    eventHubNameAndPartitions.forall {
      case (uid, ehNameAndPartitions) =>
        candidateEhNameAndPartitions.contains(uid) &&
          ehNameAndPartitions.forall(candidateEhNameAndPartitions(uid).contains)
    }
  }

  // no metadata (for backward compatibility)
  private def getLatestFileWithoutMetadata(fs: FileSystem,
                                           timestamp: Long = Long.MaxValue): Option[Path] = {
    val allFiles = fs.listStatus(progressDirectoryPath)
    if (allFiles.length < 1) {
      None
    } else {
      Some(
        allFiles
          .filter(fsStatus => fromPathToTimestamp(fsStatus.getPath) <= timestamp)
          .sortWith((f1, f2) => fromPathToTimestamp(f1.getPath) > fromPathToTimestamp(f2.getPath))(
            0)
          .getPath)
    }
  }

  private def getLatestFileWithMetadata(metadataFiles: Array[FileStatus]): Option[Path] = {
    val latestMetadata = metadataFiles
      .sortWith(
        (f1, f2) =>
          f1.getPath.getName.toLong >
            f2.getPath.getName.toLong)
      .head
    logInfo(s"locate latest timestamp in metadata as ${latestMetadata.getPath.getName}")
    Some(new Path(progressDirectoryStr + "/progress-" + latestMetadata.getPath.getName))
  }

  /**
   * get the latest progress file saved under directory
   *
   * NOTE: the additional integer in return value is to simplify the test (could be improved)
   */
  private[spark] def getLatestFile(fs: FileSystem,
                                   timestamp: Long = Long.MaxValue): (Int, Option[Path]) = {
    // first check metadata directory if exists
    if (fs.exists(metadataDirectoryPath)) {
      val metadataFiles = fs
        .listStatus(metadataDirectoryPath)
        .filter(file => file.isFile && file.getPath.getName.toLong <= timestamp)
      if (metadataFiles.nonEmpty) {
        // metadata files exists
        (0, getLatestFileWithMetadata(metadataFiles))
      } else {
        (1, getLatestFileWithoutMetadata(fs, timestamp))
      }
    } else {
      (1, getLatestFileWithoutMetadata(fs, timestamp))
    }
  }

  /**
   * this method is called when ProgressTracker is started for the first time (including recovering
   * from Spark Streaming checkpoint). This method validate the latest progress file by checking
   * whether it contains progress of all partitions we subscribe to. If not, we will delete the
   * corrupt progress file
   * @return (whether the latest file pass the validation, option to the file path,
   *         the latest timestamp)
   */
  protected def validateProgressFile(fs: FileSystem): (Boolean, Option[Path]) = {
    val (_, latestFileOpt) = getLatestFile(fs)
    val allProgressFiles = new mutable.HashMap[String, List[NameAndPartition]]
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
        val newList = allProgressFiles.getOrElseUpdate(progressRecord.uid, List[NameAndPartition]()) :+
          NameAndPartition(progressRecord.eventHubName, progressRecord.partitionId)
        allProgressFiles(progressRecord.uid) = newList
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

  protected def readProgressRecordLines(progressFilePath: Path,
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
          throw new IllegalStateException(
            s"detect corrupt progress tracking file at $line" +
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
   * pinpoint the progress file named as "progress-timestamp"
   */
  private[spark] def pinPointProgressFile(fs: FileSystem, timestamp: Long): Option[Path] = {
    try {
      require(fs.isDirectory(progressDirectoryPath), s"$progressDirectoryPath is not a directory")
      val targetFilePath = new Path(s"$progressDirectoryStr/progress-$timestamp")
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
  def read(targetConnectorUID: String, timestamp: Long, fallBack: Boolean): OffsetRecord = {
    val fs = progressDirectoryPath.getFileSystem(hadoopConfiguration)
    var recordToReturn = Map[NameAndPartition, (Long, Long)]()
    var readTimestamp: Long = 0
    var progressFileOption: Option[Path] = null
    try {
      progressFileOption = {
        if (!fallBack) {
          pinPointProgressFile(fs, timestamp)
        } else {
          getLatestFile(fs, timestamp)._2
        }
      }
      if (progressFileOption.isEmpty) {
        // if no progress file, then start from the beginning of the streams
        val connectedEventHubs = eventHubNameAndPartitions.find {
          case (connectorUID, _) => connectorUID == targetConnectorUID
        }
        require(connectedEventHubs.isDefined,
                s"cannot find $targetConnectorUID in" +
                  s" $eventHubNameAndPartitions")
        // it's hacky to take timestamp -1 as the start of streams
        readTimestamp = -1
        recordToReturn =
          connectedEventHubs.get._2.map((_, (PartitionReceiver.START_OF_STREAM.toLong, -1L))).toMap
      } else {
        val expectedTimestamp = fromPathToTimestamp(progressFileOption.get)
        val progressFilePath = progressFileOption.get
        val recordLines = readProgressRecordLines(progressFilePath, fs)
        require(recordLines.count(_.timestamp != expectedTimestamp) == 0,
                "detected inconsistent" +
                  s" progress record, expected timestamp $expectedTimestamp")
        readTimestamp = expectedTimestamp
        recordToReturn = recordLines
          .filter(progressRecord => progressRecord.uid == targetConnectorUID)
          .map(progressRecord =>
            NameAndPartition(progressRecord.eventHubName, progressRecord.partitionId) -> (progressRecord.offset, progressRecord.seqId))
          .toMap
      }
    } catch {
      case ias: IllegalArgumentException =>
        logError(ias.getMessage)
        ias.printStackTrace()
        throw ias
    }
    OffsetRecord(readTimestamp, recordToReturn)
  }

  private def createProgressFile(offsetToCommit: Map[String, Map[NameAndPartition, (Long, Long)]],
                                 fs: FileSystem,
                                 commitTime: Long): Boolean = {
    var oos: FSDataOutputStream = null
    try {
      // write progress file
      oos = fs.create(
        new Path(s"$progressDirectoryPath/${PathTools.makeProgressFileName(commitTime)}"),
        true)
      offsetToCommit.foreach {
        case (namespace, ehNameAndPartitionToOffsetAndSeq) =>
          ehNameAndPartitionToOffsetAndSeq.foreach {
            case (nameAndPartitionId, (offset, seq)) =>
              oos.writeBytes(
                ProgressRecord(commitTime,
                               namespace,
                               nameAndPartitionId.ehName,
                               nameAndPartitionId.partitionId,
                               offset,
                               seq).toString + "\n"
              )
          }
      }
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    } finally {
      if (oos != null) {
        oos.close()
      }
    }
  }

  private def createMetadata(fs: FileSystem, commitTime: Long): Boolean = {
    var oos: FSDataOutputStream = null
    try {
      oos = fs.create(
        new Path(s"$metadataDirectoryStr/" + s"${PathTools.makeMetadataFileName(commitTime)}"),
        true)
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    } finally {
      if (oos != null) {
        oos.close()
      }
    }
  }

  // write offsetToCommit to a progress tracking file
  private def transaction(offsetToCommit: Map[String, Map[NameAndPartition, (Long, Long)]],
                          fs: FileSystem,
                          commitTime: Long): Unit = {
    if (createProgressFile(offsetToCommit, fs, commitTime)) {
      if (!createMetadata(fs, commitTime)) {
        logError(s"cannot create progress file at $commitTime")
        throw new IOException(
          s"cannot create metadata file at $commitTime," +
            s" check the previous exception for the root cause")
      }
    } else {
      logError(s"cannot create progress file at $commitTime")
      throw new IOException(
        s"cannot create progress file at $commitTime," +
          s" check the previous exception for the root cause")
    }
  }

  /**
   * commit offsetToCommit to a new progress tracking file
   */
  def commit(offsetToCommit: Map[String, Map[NameAndPartition, (Long, Long)]],
             commitTime: Long): Unit = {
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

  private def allProgressRecords(timestamp: Long,
                                 ehConnectors: List[EventHubsConnector]): List[Path] = {
    val fs = tempDirectoryPath.getFileSystem(hadoopConfiguration)
    ehConnectors
      .flatMap { ehConnector =>
        ehConnector.namesAndPartitions.map(
          ehNameAndPartition =>
            PathTools.makeTempFilePath(tempDirectoryStr,
                                       ehConnector.streamId,
                                       ehConnector.uid,
                                       ehNameAndPartition,
                                       timestamp))
      }
      .filter(fs.exists)
  }

  /**
   * read progress records from temp directories
   * @return Map(Namespace -> Map(NameAndPartition -> (Offset, Seq))
   */
  def collectProgressRecordsForBatch(
      timestamp: Long,
      ehConnectors: List[EventHubsConnector]): Map[String, Map[NameAndPartition, (Long, Long)]] = {
    val records = new ListBuffer[ProgressRecord]
    val ret = new mutable.HashMap[String, Map[NameAndPartition, (Long, Long)]]
    try {
      val fs = tempDirectoryPath.getFileSystem(hadoopConfiguration)
      val files = allProgressRecords(timestamp, ehConnectors).iterator
      while (files.hasNext) {
        val file = files.next()
        val progressRecords = readProgressRecordLines(file, fs)
        records ++= progressRecords
      }
      // check timestamp consistency
      records.foreach(progressRecord =>
        if (timestamp != progressRecord.timestamp) {
          throw new IllegalStateException(
            s"detect inconsistent progress tracking file at" +
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
      val newMap = ret.getOrElseUpdate(progressRecord.uid, Map()) +
        (NameAndPartition(progressRecord.eventHubName, progressRecord.partitionId) ->
          (progressRecord.offset, progressRecord.seqId))
      ret(progressRecord.uid) = newMap
    }
    ret.toMap
  }

  def cleanProgressFile(timestampToClean: Long): Unit = {
    val fs = progressDirectoryPath.getFileSystem(hadoopConfiguration)
    val allUselessFiles = fs
      .listStatus(progressDirectoryPath, new PathFilter {
        override def accept(path: Path): Boolean = fromPathToTimestamp(path) <= timestampToClean
      })
      .map(_.getPath)
    val sortedFileList = allUselessFiles.sortWith(
      (p1, p2) =>
        fromPathToTimestamp(p1) >
          fromPathToTimestamp(p2))
    if (sortedFileList.nonEmpty) {
      sortedFileList.tail.foreach { filePath =>
        logInfo(s"delete $filePath")
        fs.delete(filePath, true)
      }
    }
    // clean temp directory
    val allUselessTempFiles = fs
      .listStatus(tempDirectoryPath, new PathFilter {
        override def accept(path: Path): Boolean = fromPathToTimestamp(path) <= timestampToClean
      })
      .map(_.getPath)
    if (allUselessTempFiles.nonEmpty) {
      allUselessTempFiles
        .groupBy(fromPathToTimestamp)
        .toList
        .sortWith((p1, p2) => p1._1 > p2._1)
        .tail
        .flatMap(_._2)
        .foreach { filePath =>
          logInfo(s"delete $filePath")
          fs.delete(filePath, true)
        }
    }
  }

  private def scheduleMetadataCleanTask(): ScheduledFuture[_] = {
    val metadataCleanTask = new Runnable {
      override def run(): Unit = {
        val fs = metadataDirectoryPath.getFileSystem(new Configuration())
        val allMetadataFiles = fs.listStatus(metadataDirectoryPath)
        val sortedMetadataFiles = allMetadataFiles.sortWith(
          (f1, f2) =>
            f1.getPath.getName.toLong <
              f2.getPath.getName.toLong)
        sortedMetadataFiles.take(math.max(sortedMetadataFiles.length - 1, 0)).map { file =>
          fs.delete(file.getPath, true)
        }
      }
    }
    // do not need to expose internals to users so hardcoded
    threadPoolForMetadataClean.scheduleAtFixedRate(metadataCleanTask, 0, 30, TimeUnit.SECONDS)
  }

  def init(): Unit
}
