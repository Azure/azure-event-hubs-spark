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

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.streaming.eventhubs.{EventHubDirectDStream, EventHubNameAndPartition, SharedUtils}

class ProgressTrackerSuite extends SharedUtils {

  override def beforeEach(): Unit = {
    super.beforeEach()
    ProgressTracker.reset()
  }

  test("progress temp directory is created properly when progress and progress temp" +
    " directory do not exist") {
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
    assert(fs.exists(progressTracker.progressDirPath))
    assert(fs.exists(progressTracker.progressTempDirPath))
  }

  test("progress temp directory is created properly when progress exists while progress" +
    " temp does not") {
    fs.mkdirs(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName)))
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
    assert(fs.exists(progressTracker.progressTempDirPath))
    assert(fs.exists(progressTracker.progressDirPath))
  }

  test("temp progress is cleaned up when a potentially partial temp progress exists") {
    val tempPath = new Path(PathTools.progressTempDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(tempPath)
    fs.create(new Path(tempPath.toString + "/temp_file"))
    val filesBefore = fs.listStatus(tempPath)
    assert(filesBefore.size === 1)
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
        new Configuration())
    assert(fs.exists(progressTracker.progressTempDirPath))
    val filesAfter = fs.listStatus(progressTracker.progressTempDirPath)
    assert(filesAfter.size === 0)
  }

  private def writeProgressFile(
      progressPath: String,
      streamId: Int,
      fs: FileSystem,
      timestamp: Long,
      namespace: String,
      ehName: String,
      partitionRange: Range,
      offset: Int,
      seq: Int): Unit = {
    for (partitionId <- partitionRange) {
      Files.write(
        Paths.get(progressPath + s"/progress-$timestamp"),
        (ProgressRecord(timestamp, namespace, streamId, ehName, partitionId, offset,
          seq).toString + "\n").getBytes, {
          if (Files.exists(Paths.get(progressPath + s"/progress-$timestamp"))) {
            StandardOpenOption.APPEND
          } else {
            StandardOpenOption.CREATE
          }
        })
    }
  }

  test("incomplete progress would be discarded") {
    createDirectStreams(ssc, "namespace1", progressRootPath.toString,
      Map("eh1" -> Map("eventhubs.partition.count" -> "1"),
        "eh2" -> Map("eventhubs.partition.count" -> "2"),
        "eh3" -> Map("eventhubs.partition.count" -> "3")))
    val progressPath = PathTools.progressDirPathStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 0)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh1", 0 to 0, 1, 1)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh2", 0 to 1, 1, 1)
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
    assert(!fs.exists(new Path(progressPath.toString + "/progress-2000")))
    assert(fs.exists(new Path(progressPath.toString + "/progress-1000")))
  }

  test("health progress file is kept") {
    // create direct streams, generate 6 EventHubAndPartitions
    val progressPath = PathTools.progressDirPathStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 0)
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
    assert(fs.exists(new Path(progressPath.toString + "/progress-1000")))
  }

  private def verifyProgressFile(
      namespace: String, ehName: String, partitionRange: Range,
      timestamp: Long, expectedOffsetAndSeq: Seq[(Long, Long)]): Unit = {
    val ehMap = progressTracker.read(namespace, timestamp)
    var expectedOffsetAndSeqIdx = 0
    for (partitionId <- partitionRange) {
      assert(ehMap(EventHubNameAndPartition(ehName, partitionId)) ===
        expectedOffsetAndSeq(expectedOffsetAndSeqIdx))
      expectedOffsetAndSeqIdx += 1
    }
  }

  test("start from the beginning of the streams when the latest progress file does not exist") {
    // generate 6 EventHubAndPartitions
    val dStream =
      createDirectStreams(ssc, "namespace1", progressRootPath.toString,
        Map("eh1" -> Map("eventhubs.partition.count" -> "1"),
          "eh2" -> Map("eventhubs.partition.count" -> "2"),
          "eh3" -> Map("eventhubs.partition.count" -> "3")))
    val dStream1 =
      createDirectStreams(ssc, "namespace2", progressRootPath.toString,
        Map("eh11" -> Map("eventhubs.partition.count" -> "1"),
          "eh12" -> Map("eventhubs.partition.count" -> "2"),
          "eh13" -> Map("eventhubs.partition.count" -> "3")))
    dStream.start()
    dStream1.start()
    val progressPath = PathTools.progressDirPathStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
    verifyProgressFile("namespace1", "eh1", 0 to 0, 1000L, Seq((-1L, -1L)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 1000L, Seq((-1L, -1L), (-1L, -1L)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 1000L, Seq((-1L, -1L), (-1L, -1L), (-1L, -1L)))
    verifyProgressFile("namespace2", "eh11", 0 to 0, 1000L, Seq((-1L, -1L)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 1000L, Seq((-1L, -1L), (-1L, -1L)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 1000L, Seq((-1L, -1L), (-1L, -1L), (-1L, -1L)))
  }

  test("the progress tracks can be read correctly") {
    // generate 6 EventHubAndPartitions
    val progressPath = PathTools.progressDirPathStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 2)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh11", 0 to 0, 1, 2)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh12", 0 to 1, 2, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh13", 0 to 2, 3, 4)

    progressTracker = ProgressTracker.initInstance(progressRootPath.toString,
      appName, new Configuration())

    verifyProgressFile("namespace1", "eh1", 0 to 0, 2000L, Seq((0, 1)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 2000L, Seq((0, 2), (0, 2)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 2000L, Seq((0, 3), (0, 3), (0, 3)))

    verifyProgressFile("namespace2", "eh11", 0 to 0, 2000L, Seq((1, 2)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 2000L, Seq((2, 3), (2, 3)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 2000L, Seq((3, 4), (3, 4), (3, 4)))
  }

  test("inconsistent timestamp in the progress tracks can be detected") {
    val progressPath = PathTools.progressDirPathStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())

    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 2)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh11", 0 to 0, 1, 2)
    // write wrong record
    Files.write(
      Paths.get(progressPath + s"/progress-1000"),
      (ProgressRecord(2000L, "namespace2", 1, "eh12", 0, 2, 3).toString + "\n").getBytes,
      StandardOpenOption.APPEND)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh12", 1 to 1, 2, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh13", 0 to 2, 3, 4)

    intercept[IllegalArgumentException] {
      progressTracker.read("namespace2", 2000L)
    }
  }

  test("snapshot progress tracking records can be read correctly") {
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString,
      appName, new Configuration())
    var progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace1",
      EventHubNameAndPartition("eh1", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace1",
      EventHubNameAndPartition("eh2", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace2",
      EventHubNameAndPartition("eh1", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 10, 20)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace2",
      EventHubNameAndPartition("eh2", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 20, 30)
    val s = progressTracker.collectProgressRecordsForBatch(1000L)
    assert(s.contains("namespace1"))
    assert(s.contains("namespace2"))
    assert(s("namespace1")(EventHubNameAndPartition("eh1", 0)) === (0, 1))
    assert(s("namespace1")(EventHubNameAndPartition("eh2", 0)) === (0, 1))
    assert(s("namespace2")(EventHubNameAndPartition("eh1", 0)) === (10, 20))
    assert(s("namespace2")(EventHubNameAndPartition("eh2", 0)) === (20, 30))
  }

  test("inconsistent timestamp in temp progress directory can be detected") {
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString,
      appName, new Configuration())
    var progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace1",
      EventHubNameAndPartition("eh1", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace1",
      EventHubNameAndPartition("eh2", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace2",
      EventHubNameAndPartition("eh1", 0), 1000L, new Configuration())
    progressWriter.write(2000L, 10, 20)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace2",
      EventHubNameAndPartition("eh2", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 20, 30)
    intercept[IllegalStateException] {
      progressTracker.collectProgressRecordsForBatch(1000L)
    }
  }

  test("latest offsets can be committed correctly and temp directory is not cleaned") {
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString,
      appName, new Configuration())

    var progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace1",
      EventHubNameAndPartition("eh1", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 0, 0)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace1",
      EventHubNameAndPartition("eh2", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 1, 1)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace2",
      EventHubNameAndPartition("eh1", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 2, 2)
    progressWriter = new ProgressWriter(progressRootPath.toString, appName, 0, "namespace2",
      EventHubNameAndPartition("eh2", 0), 1000L, new Configuration())
    progressWriter.write(1000L, 3, 3)

    val offsetToCommit = Map(
      ("namespace1", 1) -> Map(
        EventHubNameAndPartition("eh1", 0) -> (0L, 0L),
        EventHubNameAndPartition("eh2", 1) -> (1L, 1L)),
      ("namespace2", 2) -> Map(
        EventHubNameAndPartition("eh1", 3) -> (2L, 2L),
        EventHubNameAndPartition("eh2", 4) -> (3L, 3L)))
    progressTracker.commit(offsetToCommit, 1000L)
    val namespace1Offsets = progressTracker.read("namespace1", 2000L)
    assert(namespace1Offsets === Map(
      EventHubNameAndPartition("eh1", 0) -> (0L, 0L),
      EventHubNameAndPartition("eh2", 1) -> (1L, 1L)))
    val namespace2Offsets = progressTracker.read("namespace2", 2000L)
    assert(namespace2Offsets === Map(
      EventHubNameAndPartition("eh1", 3) -> (2L, 2L),
      EventHubNameAndPartition("eh2", 4) -> (3L, 3L)))
    // test temp directory cleanup
    assert(fs.exists(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName))))
    assert(fs.listStatus(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName))).length === 4)
  }

  test("locate ProgressFile correctly") {
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
    assert(progressTracker.locateProgressFile(fs, 1000L) === None)

    val progressPath = PathTools.progressDirPathStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))

    // 1000
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 2)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh11", 0 to 0, 1, 2)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh12", 0 to 1, 2, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh13", 0 to 2, 3, 4)

    // 2000
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh1", 0 to 0, 1, 2)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh2", 0 to 1, 1, 3)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh3", 0 to 2, 1, 4)
    writeProgressFile(progressPath, 1, fs, 2000L, "namespace2", "eh11", 0 to 0, 2, 3)
    writeProgressFile(progressPath, 1, fs, 2000L, "namespace2", "eh12", 0 to 1, 3, 4)
    writeProgressFile(progressPath, 1, fs, 2000L, "namespace2", "eh13", 0 to 2, 4, 5)

    // 3000
    writeProgressFile(progressPath, 0, fs, 3000L, "namespace1", "eh1", 0 to 0, 2, 3)
    writeProgressFile(progressPath, 0, fs, 3000L, "namespace1", "eh2", 0 to 1, 2, 4)
    writeProgressFile(progressPath, 0, fs, 3000L, "namespace1", "eh3", 0 to 2, 2, 5)
    writeProgressFile(progressPath, 1, fs, 3000L, "namespace2", "eh11", 0 to 0, 3, 4)
    writeProgressFile(progressPath, 1, fs, 3000L, "namespace2", "eh12", 0 to 1, 4, 5)
    writeProgressFile(progressPath, 1, fs, 3000L, "namespace2", "eh13", 0 to 2, 5, 6)

    // if latest timestamp is earlier than the specified timestamp, we shall return the latest
    // offsets
    verifyProgressFile("namespace1", "eh1", 0 to 0, 5000L, Seq((2, 3)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 5000L, Seq((2, 4), (2, 4)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 5000L, Seq((2, 5), (2, 5), (2, 5)))
    verifyProgressFile("namespace2", "eh11", 0 to 0, 5000L, Seq((3, 4)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 5000L, Seq((4, 5), (4, 5)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 5000L, Seq((5, 6), (5, 6), (5, 6)))

    // locate file correctly
    verifyProgressFile("namespace1", "eh1", 0 to 0, 3000L, Seq((1, 2)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 3000L, Seq((1, 3), (1, 3)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 3000L, Seq((1, 4), (1, 4), (1, 4)))
    verifyProgressFile("namespace2", "eh11", 0 to 0, 3000L, Seq((2, 3)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 3000L, Seq((3, 4), (3, 4)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 3000L, Seq((4, 5), (4, 5), (4, 5)))

    // locate file correctly
    verifyProgressFile("namespace1", "eh1", 0 to 0, 2000L, Seq((0, 1)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 2000L, Seq((0, 2), (0, 2)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 2000L, Seq((0, 3), (0, 3), (0, 3)))
    verifyProgressFile("namespace2", "eh11", 0 to 0, 2000L, Seq((1, 2)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 2000L, Seq((2, 3), (2, 3)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 2000L, Seq((3, 4), (3, 4), (3, 4)))

    assert(progressTracker.locateProgressFile(fs, 1000L) === None)
    assert(progressTracker.locateProgressFile(fs, 500L) === None)
  }
}
