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

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Assertions, BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.eventhubs.EventHubNameAndPartition

class DfsBasedOffsetStore2Suite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  private val appName = "dummyapp"
  private val streamId = 0
  private val nameSpace = "eventhubs"

  var offsetStore: DfsBasedOffsetStore2 = _
  var checkpointRootPath: Path = _
  var fs: FileSystem = _

  before {
    checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    fs = checkpointRootPath.getFileSystem(new Configuration())
  }

  after {
    offsetStore = null
  }

  test("checkpoint temp directory is created properly when checkpoint and checkpoint temp" +
    " directory do not exist") {
    offsetStore = OffsetStoreNew.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    assert(fs.exists(offsetStore.checkpointTempDirPath))
    assert(!fs.exists(offsetStore.checkpointDirPath))
  }

  test("checkpoint temp directory is created properly when checkpoint exists while checkpoint" +
    " temp does not") {
    fs.mkdirs(new Path(PathTools.tempCheckpointDirStr(checkpointRootPath.toString + "/checkpoint",
      appName, streamId, nameSpace)))
    offsetStore = OffsetStoreNew.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    assert(fs.exists(offsetStore.checkpointTempDirPath))
    assert(!fs.exists(offsetStore.checkpointDirPath))
  }

  test("temp checkpoint is cleaned up when a potentially partial temp checkpoint exists") {
    val tempPath = new Path(PathTools.tempCheckpointDirStr(checkpointRootPath.toString +
      "/checkpoint", appName, streamId, nameSpace))
    fs.mkdirs(tempPath)
    fs.create(new Path(tempPath.toString + "/temp_file"))
    val filesBefore = fs.listStatus(tempPath)
    assert(filesBefore.size === 1)
    offsetStore = OffsetStoreNew.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    assert(fs.exists(offsetStore.checkpointTempDirPath))
    val filesAfter = fs.listStatus(offsetStore.checkpointTempDirPath)
    assert(filesAfter.size === 0)
  }

  test("checkpoint directory can be constructed correctly") {
    offsetStore = OffsetStoreNew.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    for (time <- List(2000, 3000, 4000)) {
      offsetStore.write(Time(time), EventHubNameAndPartition(nameSpace, 0), 0, 0)
      offsetStore.write(Time(time), EventHubNameAndPartition(nameSpace, 1), 100, 100)
      offsetStore.write(Time(time), EventHubNameAndPartition(nameSpace, 2), 200, 200)
      offsetStore.write(Time(time), EventHubNameAndPartition(nameSpace, 3), 300, 300)
      offsetStore.commit(Time(time))
    }
    // tests
    assert(fs.exists(offsetStore.checkpointDirPath))
    assert(fs.exists(offsetStore.checkpointTempDirPath))
    assert(fs.listStatus(offsetStore.checkpointTempDirPath).isEmpty)
    assert(fs.exists(offsetStore.checkpointBackupDirPath))
    assert(fs.exists(new Path(offsetStore.checkpointBackupDirPath.toString +
      s"/$nameSpace-2000")))
    assert(fs.exists(new Path(offsetStore.checkpointBackupDirPath.toString +
      s"/$nameSpace-3000")))
  }

  test("inconsistent checkpoint file is detected") {
    offsetStore = OffsetStoreNew.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    offsetStore.write(Time(1000), EventHubNameAndPartition(nameSpace, 0), 0, 0)
    offsetStore.write(Time(2000), EventHubNameAndPartition(nameSpace, 1), 100, 100)
    offsetStore.write(Time(2000), EventHubNameAndPartition(nameSpace, 2), 200, 200)
    offsetStore.write(Time(2000), EventHubNameAndPartition(nameSpace, 3), 300, 300)
    offsetStore.commit(Time(2000))
    val caught = intercept[RuntimeException] {
      offsetStore.read()
    }
    assert(caught.getMessage == s"detect inconsistent checkpoint at 1000 $nameSpace 0 0 0," +
      s" expected timestamp: 2000," +
      s" it might be a bug in the implementation of underlying file system")
  }
}
