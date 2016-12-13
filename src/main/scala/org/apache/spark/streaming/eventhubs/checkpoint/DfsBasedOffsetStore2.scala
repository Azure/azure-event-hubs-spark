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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.eventhubs.EventHubNameAndPartition

/**
 * the offsetStore which reads checkpoint directory, writes temp checkpoint files and atomically
 * commits the temp checkpoints
 * @param checkpointDir the directory of checkpoint files
 * @param appName the name of Spark application
 * @param streamId the id of input stream
 * @param namespace the namespace of eventhubs
 * @param hadoopConfiguration the hadoop configuration instance
 * @param runOnDriver whether the OffsetStore runs on driver or executor
 */
private[eventhubs] class DfsBasedOffsetStore2 private[checkpoint] (
    checkpointDir: String,
    appName: String,
    streamId: Int,
    namespace: String,
    private val hadoopConfiguration: Configuration,
    runOnDriver: Boolean = true) extends OffsetStoreDirectStreaming with Logging {

  private val checkpointDirStr = PathTools.checkpointDirPathStr(checkpointDir, appName,
    streamId, namespace)
  private val checkpointTempDirStr = PathTools.tempCheckpointDirStr(checkpointDir,
    appName, streamId, namespace)
  private val checkpointBackupDirStr = PathTools.checkpointBackupDirStr(checkpointDir,
    appName, streamId)

  private[eventhubs] val checkpointDirPath = new Path(checkpointDirStr)
  private[eventhubs] val checkpointTempDirPath = new Path(checkpointTempDirStr)
  private[eventhubs] val checkpointBackupDirPath = new Path(checkpointBackupDirStr)

  // the lock synchronizing the read and committing operations, since they are executed in driver
  // and listener thread respectively.
  private val driverLock = new Object

  /**
   * in this method, we have to cleanup the partially executed commit operation
   * there are four steps in a commit
   *
   * step 1. move the checkpoint path to backup dir
   * step 2. rename the temp checkpoint path (latest checkpoint path) to checkpoint path
   * step 3. create empty temp checkpoint path
   * step 4. create timestamp file in the directory
   *
   * NOTE: we rely on the atomic implementation of these operations in HDFS
   *
   * we describe the state of the commit execution with a 2-tuple
   * (checkpointDirExisted, checkpointTempDirExisted)
   *
   * the strategy to recover from a failure is that, when we found an existing temp checkpoint file
   * we should conservatively delete it to prevent a partially written checkpoint file being
   * committed; otherwise, we just create temp checkpoint directory to facilitate the further
   * processing
   */
  override protected[checkpoint] def init(): Unit = {
    // recover from partially executed checkpoint commit
    if (runOnDriver) {
      val fs = checkpointDirPath.getFileSystem(hadoopConfiguration)
      try {
        val checkpointDirExisted = fs.exists(checkpointDirPath)
        val checkpointTempDirExisted = fs.exists(checkpointTempDirPath)
        val checkpointBackupDirExisted = fs.exists(checkpointBackupDirPath)
        if (!checkpointBackupDirExisted) {
          fs.mkdirs(checkpointBackupDirPath)
        }
        (checkpointDirExisted, checkpointTempDirExisted) match {
          case (false, false) | (true, false) =>
            // start the program for the first time or the third step is failed
            fs.mkdirs(checkpointTempDirPath)
          case (true, true) | (false, true) =>
            if (fs.listStatus(checkpointTempDirPath).length > 0) {
              // failed at step 1 or a partial checkpoint is written
              // we shall not trust the temp checkpoint
              fs.delete(checkpointTempDirPath, true)
              logInfo(s"cleanup temp checkpoint $checkpointTempDirPath")
              fs.mkdirs(checkpointTempDirPath)
            }
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

  override def write(
      time: Time,
      eventHubNameAndPartition: EventHubNameAndPartition,
      cpOffset: Long,
      cpSeq: Long): Unit = {
    val fs = checkpointDirPath.getFileSystem(hadoopConfiguration)
    var cpFileStream: FSDataOutputStream = null
    try {
      // it shall be safe to overwrite checkpoint, since we will not start a new job when
      // checkpoint hasn't been committed
      cpFileStream = fs.create(
        new Path(s"$checkpointTempDirStr/${eventHubNameAndPartition.toString}"), true)
      cpFileStream.writeBytes(s"${time.milliseconds} ${eventHubNameAndPartition.eventHubName}" +
        s" ${eventHubNameAndPartition.partitionId} $cpOffset $cpSeq")
      logDebug(s"${time.milliseconds} ${eventHubNameAndPartition.eventHubName}" +
        s" ${eventHubNameAndPartition.partitionId} $cpOffset $cpSeq")
    } catch {
      case ioe: IOException =>
        ioe.printStackTrace()
        throw ioe
    } finally {
      try {
        if (cpFileStream != null) {
          cpFileStream.close()
        }
      } catch {
        case ioe: IOException =>
          ioe.printStackTrace()
          throw new Exception(s"file system corrupt in $eventHubNameAndPartition")
      }
    }
  }

  private def fetchCheckpointTimestamp(fs: FileSystem): Long = {
    val tsFile = fs.open(new Path(checkpointDirPath.toString + "/timestamp"))
    val timestamp = tsFile.readLong()
    tsFile.close()
    timestamp
  }

  override def read(): Map[EventHubNameAndPartition, (Long, Long)] = driverLock.synchronized {
    require(runOnDriver, "read operation is supposed to be executed only on driver")
    val fs = checkpointDirPath.getFileSystem(hadoopConfiguration)
    val ret = new mutable.HashMap[EventHubNameAndPartition, (Long, Long)]
    var br: BufferedReader = null
    if (fs.exists(checkpointDirPath)) {
      // read timestamp
      val timestamp = fetchCheckpointTimestamp(fs)
      val files = fs.listFiles(checkpointDirPath, false)
      try {
        while (files.hasNext) {
          val file = files.next()
          val fileName = file.getPath.getName
          if (fileName != "timestamp") {
            val inputStream = fs.open(file.getPath)
            br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
            val line = br.readLine()
            if (line == null) {
              logError(s"corrupted checkpoint file in ${file.getPath}")
              throw new IllegalStateException(s"corrupted checkpoint file in ${file.getPath}" +
                s", it might be a bug in the implementation of underlying file system")
            }
            val Array(batchTime, eventHubName, partitionID, offsetStr, seqNumStr) = line.split(" ")
            if (batchTime.toLong != timestamp) {
              throw new IllegalStateException(s"detect inconsistent checkpoint at $line, expected" +
                s" timestamp: $timestamp, it might be a bug in the implementation of" +
                s" underlying file system")
            }
            ret += EventHubNameAndPartition(eventHubName, partitionID.toInt) -> (offsetStr.toLong,
              seqNumStr.toLong)
            br.close()
          }
        }
      } catch {
        case ioe: IOException =>
          ioe.printStackTrace()
          throw ioe
        case ise: IllegalStateException =>
          ise.printStackTrace()
          throw ise
      } finally {
        if (br != null) {
          br.close()
        }
      }
    }
    ret.toMap
  }

  override def close(): Unit = {

  }

  private def backupLocation(fs: FileSystem): Path = {
    val ins = fs.open(new Path(checkpointDirPath.toString + "/timestamp"))
    val backupTimestamp = ins.readLong()
    ins.close()
    new Path(checkpointBackupDirStr + s"/$namespace-$backupTimestamp")
  }

  private def transaction(fs: FileSystem, time: Time): Unit = {
    if (fs.exists(checkpointDirPath)) {
      val backupTarget = backupLocation(fs)
      fs.rename(checkpointDirPath, backupTarget)
    }
    if (fs.listStatus(checkpointTempDirPath).length > 0) {
      fs.rename(checkpointTempDirPath, checkpointDirPath)
      fs.mkdirs(checkpointTempDirPath)
      // write timestamp file
      var oos: FSDataOutputStream = null
      try {
        oos = fs.create(new Path(checkpointDirPath.toString + "/timestamp"))
        oos.writeLong(time.milliseconds)
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
  }

  override def commit(commitTime: Time): Unit = driverLock.synchronized {
    require(runOnDriver, "commit operation is supposed to be executed only on driver")
    val fs = new Path(checkpointDir).getFileSystem(hadoopConfiguration)
    try {
      transaction(fs, commitTime)
    } catch {
      case ioe: IOException =>
        ioe.printStackTrace()
        throw ioe
    } finally {
      // EMPTY, we leave the cleanup of partially executed transaction to the moment when recovering
      // from failure
    }
  }
}
