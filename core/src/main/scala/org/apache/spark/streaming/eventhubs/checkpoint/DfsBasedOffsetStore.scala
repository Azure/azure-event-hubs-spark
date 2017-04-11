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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.SparkContext


/**
 * A DFS based OffsetStore implementation
 */
@SerialVersionUID(1L)
class DfsBasedOffsetStore(
    directory: String,
    namespace: String,
    name: String,
    partition: String) extends OffsetStore with Logging {

  if (!SparkContext.getOrCreate().isLocal) {
    require(directory.startsWith("hdfs://") || directory.startsWith("adl://"),
      "we only support to store offset in HDFS/ADLS when running Spark in non-local mode ")
  }

  var path: Path = _
  var backupPath: Path = _
  var checkpointFile: FileSystem = _
  var backupCheckpointFile: FileSystem = _

  /**
   * Open two files, the actual checkpoint file and the backup checkpoint file
   */

  override def open(): Unit = {
    if (checkpointFile == null) {
      path = new Path(directory + "/" + namespace + "/" + name + "/" + partition)
      checkpointFile = path.getFileSystem(new Configuration())
    }

    if (backupCheckpointFile == null) {
      backupPath = new Path(directory + "/" + namespace + "/" + name + "/" + partition + ".bk")
      backupCheckpointFile = backupPath.getFileSystem(new Configuration())
    }
  }

  /**
   * @param offset
   * Write happens in three steps - one read and two writes. The first is a read attempt made on
   * the actual checkpoint file. This is to ensure that the checkpoint file contains valid offset.
   * If successful it means the actual checkpoint file can be updated only if the backup checkpoint
   * update is successful. The second is a write attempt on the backup checkpoint file. Once that
   * write is successful or the read of the actual checkpoint file was unsuccessful the third is a
   * write attempt on the actual checkpoint file. In case of any failure at the time of write at
   * least one file will contain a valid offset value which in the worst case will be a previous
   * offset value (if one or more of them had valid offset values to begin with). The at least once
   * guarantee still holds.
   */
  override def write(offset: String): Unit = {

    var readSuccessful: Boolean = false
    var writeSuccessful: Boolean = false

    if (checkpointFile.exists(path)) {
      val stream = checkpointFile.open(path)
      try {
        stream.readUTF()
        readSuccessful = true
      } catch {
        case e: Exception =>
          logTrace(s"Failed to read offset from checkpoint file $path before write.", e)
      } finally {
        stream.close()
      }
    }

    if (readSuccessful) {
      val backupStream = backupCheckpointFile.create(backupPath, true)
      try {
        backupStream.writeUTF(offset)
        writeSuccessful = true
      } catch {
        case e: Exception =>
          logError(s"Failed to write offset to backup checkpoint file $backupPath", e)
      } finally {
        backupStream.close()
      }
    }

    if (writeSuccessful || !readSuccessful) {
      val stream = checkpointFile.create(path, true)
      try {
        stream.writeUTF(offset)
        writeSuccessful = true
      } catch {
        case e: Exception => logError(s"Failed to write offset to checkpoint file $path.", e)
      } finally {
        stream.close()
      }
    }

    if (!writeSuccessful) {
      throw new Exception(s"Failed to write offset information for partition $partition.")
    }
  }

  /**
   * Read happens in two steps. The first read attempt happens on the actual checkpoint file.
   * There are three possible situations:
   * 1.1) The actual checkpoint directory does not exist.
   * 1.2) The actual checkpoint directory exists but empty.
   * 1.3) The actual checkpoint directory exists and contains offset information.
   * For case 1.3) offset is read and the job continues. For cases 1.1) and 1.2) the second read
   * attempt happens on the backup checkpoint file. There are again three possible situations:
   * 2.1) The backup checkpoint directory does not exist.
   * 2.2) The backup checkpoint directory exists but empty.
   * 2.3) The backup checkpoint directory exists and contains offset information.
   * The possible actions for the combination of events 1.1, 1.2, 2.1, 2.2, 2.3 are listed below:
   * 1.1 + 2.1: Start from starting offset (-1).
   * 1.1 + 2.2: Cannot happen.
   * 1.1 + 2.3: Start from the offset in the backup checkpoint file.
   * 1.2 + 2.1: Cannot happen.
   * 1.2 + 2.2: Cannot happen.
   * 1.2 + 2.3: Start from the offset in the backup checkpoint file.
   */
  override def read(): String = {

    var fileExists: Boolean = false
    var readSuccessful: Boolean = false

    var offset: String = "-1"

    if (checkpointFile.exists(path)) {
      fileExists = true
      val stream = checkpointFile.open(path)
      try {
        offset = stream.readUTF()
        readSuccessful = true
      } catch {
        case e: Exception => logError(s"Failed to read offset from checkpoint file $path.", e)
      } finally {
        stream.close()
      }
    }

    if (!readSuccessful) {
      if (backupCheckpointFile.exists(backupPath)) {
        fileExists = true
        val backupStream = backupCheckpointFile.open(backupPath)
        try {
          offset = backupStream.readUTF()
          readSuccessful = true
        } catch {
          case e: Exception =>
            logError(s"Failed to read offset from backup checkpoint file $backupPath.")
        } finally {
          backupStream.close()
        }
      }
    }

    if (fileExists && !readSuccessful) {
      throw new Exception(s"Failed to read offset information for partition $partition.")
    }
    offset
  }

  override def close(): Unit = {
    // pass
  }
}

