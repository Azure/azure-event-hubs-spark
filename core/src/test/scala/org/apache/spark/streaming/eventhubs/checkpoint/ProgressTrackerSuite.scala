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

import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.eventhubscommon.{EventHubNameAndPartition, EventHubsConnector, OffsetRecord}
import org.apache.spark.eventhubscommon.progress.{PathTools, ProgressRecord, ProgressWriter}
import org.apache.spark.streaming.eventhubs.SharedUtils

class ProgressTrackerSuite extends SharedUtils {

  class DummyEventHubsConnector(
      sId: Int,
      uniqueId: String,
      connedInstances: List[EventHubNameAndPartition]) extends EventHubsConnector {
    override def streamId: Int = sId

    override def uid: String = uniqueId

    override def connectedInstances: List[EventHubNameAndPartition] = connedInstances
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    DirectDStreamProgressTracker.reset()
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
      val filePath = Paths.get(progressPath + s"/${PathTools.makeProgressFileName(timestamp)}")
      val stdOpenOption = if (Files.exists(filePath)) {
        StandardOpenOption.APPEND
      } else {
        StandardOpenOption.CREATE
      }

      Files.write(filePath,
        s"${ProgressRecord(timestamp, namespace, ehName, partitionId, offset, seq)
          .toString}\n".getBytes,
        stdOpenOption)
    }
  }

  private def createMetadataFile(fs: FileSystem, metadataPath: String, timestamp: Long): Unit =
    fs.create(new Path(s"$metadataPath/${PathTools.makeMetadataFileName(timestamp)}"))

  test("progress temp directory is created properly when progress and progress temp" +
    " directory do not exist") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    assert(fs.exists(progressTracker.progressDirectoryPath))
    assert(fs.exists(progressTracker.tempDirectoryPath))
  }

  test("progress temp directory is created properly when progress exists while progress" +
    " temp does not") {
    fs.mkdirs(PathTools.makeTempDirectoryPath(progressRootPath.toString, appName))
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    assert(fs.exists(progressTracker.progressDirectoryPath))
    assert(fs.exists(progressTracker.tempDirectoryPath))
  }

  test("temp progress is cleaned up when a potentially partial temp progress exists") {
    val tempPath = PathTools.makeTempDirectoryPath(progressRootPath.toString + "/progress", appName)
    fs.mkdirs(tempPath)
    fs.create(new Path(tempPath.toString + "/temp_file"))
    val filesBefore = fs.listStatus(tempPath)
    assert(filesBefore.size === 1)
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    assert(fs.exists(progressTracker.tempDirectoryPath))
    val filesAfter = fs.listStatus(progressTracker.tempDirectoryPath)
    assert(filesAfter.size === 0)
  }

  test("incomplete progress would be discarded") {
    createDirectStreams(ssc, "namespace1", progressRootPath.toString,
      Map("eh1" -> Map("eventhubs.partition.count" -> "1"),
          "eh2" -> Map("eventhubs.partition.count" -> "2"),
          "eh3" -> Map("eventhubs.partition.count" -> "3")))

    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))

    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 0)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh1", 0 to 0, 1, 1)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh2", 0 to 1, 1, 1)

    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    assert(!fs.exists(new Path(progressPath + "/progress-2000")))
    assert(fs.exists(new Path(progressPath + "/progress-1000")))
  }

  test("health progress file is kept") {
    // create direct streams, generate 6 EventHubAndPartitions
    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))

    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 0)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 0)

    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    assert(fs.exists(new Path(progressPath + "/progress-1000")))
  }

  private def verifyProgressFile(
      namespace: String, ehName: String, partitionRange: Range,
      timestamp: Long, expectedOffsetAndSeq: Seq[(Long, Long)]): Unit = {
    val ehMap = progressTracker.asInstanceOf[DirectDStreamProgressTracker]
      .read(namespace, timestamp - 1000L, fallBack = false)
    var expectedOffsetAndSeqIdx = 0
    for (partitionId <- partitionRange) {
      assert(ehMap.offsets(EventHubNameAndPartition(ehName, partitionId)) ===
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

    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    verifyProgressFile("namespace1", "eh1", 0 to 0, 1000L, Seq((-1L, -1L)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 1000L, Seq((-1L, -1L), (-1L, -1L)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 1000L, Seq((-1L, -1L), (-1L, -1L), (-1L, -1L)))
    verifyProgressFile("namespace2", "eh11", 0 to 0, 1000L, Seq((-1L, -1L)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 1000L, Seq((-1L, -1L), (-1L, -1L)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 1000L, Seq((-1L, -1L), (-1L, -1L), (-1L, -1L)))
  }

  test("the progress tracks can be read correctly") {
    // generate 6 EventHubAndPartitions
    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))

    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 2)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh11", 0 to 0, 1, 2)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh12", 0 to 1, 2, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh13", 0 to 2, 3, 4)

    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    verifyProgressFile("namespace1", "eh1", 0 to 0, 2000L, Seq((0, 1)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 2000L, Seq((0, 2), (0, 2)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 2000L, Seq((0, 3), (0, 3), (0, 3)))

    verifyProgressFile("namespace2", "eh11", 0 to 0, 2000L, Seq((1, 2)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 2000L, Seq((2, 3), (2, 3)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 2000L, Seq((3, 4), (3, 4), (3, 4)))
  }

  test("inconsistent timestamp in the progress tracks can be detected") {
    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2", 0 to 1, 0, 2)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3", 0 to 2, 0, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh11", 0 to 0, 1, 2)

    // write wrong record
    Files.write(
      Paths.get(progressPath + s"/progress-1000"),
      (ProgressRecord(2000L, "namespace2", "eh12", 0, 2, 3).toString + "\n").getBytes,
      StandardOpenOption.APPEND)

    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh12", 1 to 1, 2, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh13", 0 to 2, 3, 4)

    intercept[IllegalArgumentException] {
      progressTracker.asInstanceOf[DirectDStreamProgressTracker]
        .read("namespace2", 1000L, fallBack = false)
    }
  }

  test("snapshot progress tracking records can be read correctly") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    val eh1Partition0 = EventHubNameAndPartition("eh1", 0)
    val eh2Partition0 = EventHubNameAndPartition("eh2", 0)
    val connectedInstances = List(eh1Partition0, eh2Partition0)
    val connector1 = new DummyEventHubsConnector(0, "namespace1", connectedInstances)
    val connector2 = new DummyEventHubsConnector(0, "namespace2", connectedInstances)

    var progressWriter = new ProgressWriter(0, "namespace1", eh1Partition0,
      1000L, new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(0, "namespace1", eh2Partition0, 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(0, "namespace2", eh1Partition0, 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 10, 20)
    progressWriter = new ProgressWriter(0, "namespace2", eh2Partition0, 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 20, 30)
    val s = progressTracker.asInstanceOf[DirectDStreamProgressTracker]
      .collectProgressRecordsForBatch(1000L, List(connector1, connector2))

    assert(s.contains("namespace1"))
    assert(s.contains("namespace2"))
    assert(s("namespace1")(EventHubNameAndPartition("eh1", 0)) === (0, 1))
    assert(s("namespace1")(EventHubNameAndPartition("eh2", 0)) === (0, 1))
    assert(s("namespace2")(EventHubNameAndPartition("eh1", 0)) === (10, 20))
    assert(s("namespace2")(EventHubNameAndPartition("eh2", 0)) === (20, 30))
  }

  test("inconsistent timestamp in temp progress directory can be detected") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    val eh1Partition0 = EventHubNameAndPartition("eh1", 0)
    val eh2Partition0 = EventHubNameAndPartition("eh2", 0)
    val connectedInstances = List(eh1Partition0, eh2Partition0)
    val connector1 = new DummyEventHubsConnector(0, "namespace1", connectedInstances)
    val connector2 = new DummyEventHubsConnector(0, "namespace2", connectedInstances)

    var progressWriter = new ProgressWriter(0, "namespace1", eh1Partition0,
      1000L, new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(0, "namespace1", eh2Partition0, 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 0, 1)
    progressWriter = new ProgressWriter(0, "namespace2", eh1Partition0, 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(2000L, 10, 20)
    progressWriter = new ProgressWriter(0, "namespace2", eh2Partition0, 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 20, 30)

    intercept[IllegalStateException] {
      progressTracker.asInstanceOf[DirectDStreamProgressTracker].
        collectProgressRecordsForBatch(1000L, List(connector1, connector2))
    }
  }

  test("latest offsets can be committed correctly and temp directory is not cleaned") {
    progressTracker = DirectDStreamProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())

    var progressWriter = new ProgressWriter(0, "namespace1", EventHubNameAndPartition("eh1", 0),
      1000L, new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 0, 0)
    progressWriter = new ProgressWriter(0, "namespace1", EventHubNameAndPartition("eh2", 0), 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 1, 1)
    progressWriter = new ProgressWriter(0, "namespace2", EventHubNameAndPartition("eh1", 0), 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 2, 2)
    progressWriter = new ProgressWriter(0, "namespace2", EventHubNameAndPartition("eh2", 0), 1000L,
      new Configuration(), progressRootPath.toString, appName)
    progressWriter.write(1000L, 3, 3)

    val offsetToCommit = Map(
      "namespace1" -> Map(
        EventHubNameAndPartition("eh1", 0) -> (0L, 0L),
        EventHubNameAndPartition("eh2", 1) -> (1L, 1L)),
      "namespace2" -> Map(
        EventHubNameAndPartition("eh1", 3) -> (2L, 2L),
        EventHubNameAndPartition("eh2", 4) -> (3L, 3L)))
    progressTracker.asInstanceOf[DirectDStreamProgressTracker].commit(offsetToCommit, 1000L)
    val namespace1Offsets = progressTracker.asInstanceOf[DirectDStreamProgressTracker]
      .read("namespace1", 1000L, fallBack = false)
    assert(namespace1Offsets === OffsetRecord(1000L, Map(
      EventHubNameAndPartition("eh1", 0) -> (0L, 0L),
      EventHubNameAndPartition("eh2", 1) -> (1L, 1L))))
    val namespace2Offsets = progressTracker.asInstanceOf[DirectDStreamProgressTracker]
      .read("namespace2", 1000L, fallBack = false)
    assert(namespace2Offsets === OffsetRecord(1000L, Map(
      EventHubNameAndPartition("eh1", 3) -> (2L, 2L),
      EventHubNameAndPartition("eh2", 4) -> (3L, 3L))))

    // test temp directory cleanup
    assert(fs.exists(PathTools.makeTempDirectoryPath(
      progressRootPath.toString, appName)))
    assert(fs.listStatus(PathTools.makeTempDirectoryPath(
      progressRootPath.toString, appName)).length === 4)
  }

  test("locate ProgressFile correctly") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    assert(progressTracker.asInstanceOf[DirectDStreamProgressTracker]
      .pinPointProgressFile(fs, 1000L) === None)

    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))

    // 1000
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1",
      0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh2",
      0 to 1, 0, 2)
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh3",
      0 to 2, 0, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh11",
      0 to 0, 1, 2)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh12",
      0 to 1, 2, 3)
    writeProgressFile(progressPath, 1, fs, 1000L, "namespace2", "eh13",
      0 to 2, 3, 4)

    // 2000
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh1",
      0 to 0, 1, 2)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh2",
      0 to 1, 1, 3)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh3",
      0 to 2, 1, 4)
    writeProgressFile(progressPath, 1, fs, 2000L, "namespace2", "eh11",
      0 to 0, 2, 3)
    writeProgressFile(progressPath, 1, fs, 2000L, "namespace2", "eh12",
      0 to 1, 3, 4)
    writeProgressFile(progressPath, 1, fs, 2000L, "namespace2", "eh13",
      0 to 2, 4, 5)

    // 3000
    writeProgressFile(progressPath, 0, fs, 3000L, "namespace1", "eh1",
      0 to 0, 2, 3)
    writeProgressFile(progressPath, 0, fs, 3000L, "namespace1", "eh2",
      0 to 1, 2, 4)
    writeProgressFile(progressPath, 0, fs, 3000L, "namespace1", "eh3",
      0 to 2, 2, 5)
    writeProgressFile(progressPath, 1, fs, 3000L, "namespace2", "eh11",
      0 to 0, 3, 4)
    writeProgressFile(progressPath, 1, fs, 3000L, "namespace2", "eh12",
      0 to 1, 4, 5)
    writeProgressFile(progressPath, 1, fs, 3000L, "namespace2", "eh13",
      0 to 2, 5, 6)

    // if latest timestamp is earlier than the specified timestamp,
    // then we shall return the latest offsets
    verifyProgressFile("namespace1", "eh1", 0 to 0, 4000L, Seq((2, 3)))
    verifyProgressFile("namespace1", "eh2", 0 to 1, 4000L, Seq((2, 4), (2, 4)))
    verifyProgressFile("namespace1", "eh3", 0 to 2, 4000L, Seq((2, 5), (2, 5), (2, 5)))
    verifyProgressFile("namespace2", "eh11", 0 to 0, 4000L, Seq((3, 4)))
    verifyProgressFile("namespace2", "eh12", 0 to 1, 4000L, Seq((4, 5), (4, 5)))
    verifyProgressFile("namespace2", "eh13", 0 to 2, 4000L, Seq((5, 6), (5, 6), (5, 6)))

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
  }

  test("read progress file correctly when metadata exists") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    val metadataPath = PathTools.makeMetadataDirectoryStr(progressRootPath.toString, appName)
    createMetadataFile(fs, metadataPath, 1000L)
    val result = progressTracker.read("namespace1", 1000, fallBack = true)

    assert(result.timestamp == 1000L)
    assert(result.offsets == Map(EventHubNameAndPartition("eh1", 0) -> (0, 1)))
  }

  test("ProgressTracker does query metadata when metadata exists") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())

    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))
    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh1", 0 to 0, 0, 1)
    val metadataPath = PathTools.makeMetadataDirectoryStr(progressRootPath.toString, appName)
    createMetadataFile(fs, metadataPath, 1000L)
    createMetadataFile(fs, metadataPath, 2000L)
    val (sourceOfLatestFile, result) = progressTracker.getLatestFile(fs)

    assert(sourceOfLatestFile === 0)
    assert(result.isDefined)
    assert(result.get.getName === "progress-2000")
  }

  test("When metadata presents, we should respect it even the more recent progress file is there") {
    progressTracker = DirectDStreamProgressTracker
      .initInstance(progressRootPath.toString, appName, new Configuration())
    val progressPath = PathTools.makeProgressDirectoryStr(progressRootPath.toString, appName)
    fs.mkdirs(new Path(progressPath))

    writeProgressFile(progressPath, 0, fs, 1000L, "namespace1", "eh1", 0 to 0, 0, 1)
    writeProgressFile(progressPath, 0, fs, 2000L, "namespace1", "eh1", 0 to 0, 0, 1)
    val metadataPath = PathTools.makeMetadataDirectoryStr(progressRootPath.toString, appName)
    createMetadataFile(fs, metadataPath, 1000L)
    val (sourceOfLatestFile, result) = progressTracker.getLatestFile(fs)

    assert(sourceOfLatestFile === 0)
    assert(result.isDefined)
    assert(result.get.getName === "progress-1000")
  }
}
