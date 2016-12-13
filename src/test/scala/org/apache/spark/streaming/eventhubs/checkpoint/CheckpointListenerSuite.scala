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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.streaming.eventhubs.EventHubNameAndPartition

// scalastyle:off
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamInputInfo, StreamingListenerBatchCompleted}
// scalastyle:on

class CheckpointListenerSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  val appName = "dummyapp"
  val streamId = 0
  val nameSpace = "eventhubs"

  var fs: FileSystem = _
  var checkpointRootPath: Path = _

  before {
    checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    fs = checkpointRootPath.getFileSystem(new Configuration())
  }

  test("checkpoint files are created correctly") {
    val checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    val offsetStore = OffsetStoreDirectStreaming.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    offsetStore.write(Time(10), EventHubNameAndPartition(nameSpace, 0), 0, 0)
    offsetStore.write(Time(10), EventHubNameAndPartition(nameSpace, 1), 100, 100)
    offsetStore.write(Time(10), EventHubNameAndPartition(nameSpace, 2), 200, 200)
    offsetStore.write(Time(10), EventHubNameAndPartition(nameSpace, 3), 300, 300)
    val listener = new CheckpointListener
    val batchCompleted = StreamingListenerBatchCompleted(
      BatchInfo(
        Time(10),
        Map(streamId -> StreamInputInfo(streamId, 400)),
        submissionTime = 0L,
        None,
        None,
        Map()))
    listener.onBatchCompleted(batchCompleted)
    offsetStore.write(Time(20), EventHubNameAndPartition(nameSpace, 0), 0, 0)
    offsetStore.write(Time(20), EventHubNameAndPartition(nameSpace, 1), 100, 100)
    offsetStore.write(Time(20), EventHubNameAndPartition(nameSpace, 2), 200, 200)
    offsetStore.write(Time(20), EventHubNameAndPartition(nameSpace, 3), 300, 300)
    val batchCompleted2 = StreamingListenerBatchCompleted(
      BatchInfo(
        Time(20),
        Map(streamId -> StreamInputInfo(streamId, 400)),
        submissionTime = 0L,
        None,
        None,
        Map()))
    listener.onBatchCompleted(batchCompleted2)
    assert(fs.exists(offsetStore.checkpointDirPath))
    assert(fs.exists(offsetStore.checkpointTempDirPath))
    assert(fs.listStatus(offsetStore.checkpointTempDirPath).isEmpty)
    assert(fs.exists(offsetStore.checkpointBackupDirPath))
    assert(fs.exists(new Path(offsetStore.checkpointBackupDirPath.toString +
      s"/$nameSpace-10")))
  }

  test("empty batch is skipped properly") {
    val checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    val offsetStore = OffsetStoreDirectStreaming.newInstance(
      checkpointRootPath.toString + "/checkpoint", appName, streamId,
      nameSpace, new Configuration()).asInstanceOf[DfsBasedOffsetStore2]
    val listener = new CheckpointListener
    val batchCompleted = StreamingListenerBatchCompleted(
      BatchInfo(
        Time(10),
        Map(streamId -> StreamInputInfo(streamId, 0)),
        0,
        None,
        None,
        Map()))
    listener.onBatchCompleted(batchCompleted)
    assert(!fs.exists(offsetStore.checkpointDirPath))
    assert(fs.exists(offsetStore.checkpointTempDirPath))
    assert(fs.listStatus(offsetStore.checkpointTempDirPath).isEmpty)
    assert(fs.exists(offsetStore.checkpointBackupDirPath))
    assert(!fs.exists(new Path(offsetStore.checkpointBackupDirPath.toString +
      s"/$nameSpace-10")))
  }
}
