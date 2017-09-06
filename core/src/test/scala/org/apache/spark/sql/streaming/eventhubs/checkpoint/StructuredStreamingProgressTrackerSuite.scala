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

package org.apache.spark.sql.streaming.eventhubs.checkpoint

import java.nio.file.Files
import java.time.Instant

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.eventhubscommon._
import org.apache.spark.eventhubscommon.progress._
import org.apache.spark.sql.streaming.eventhubs.EventHubsSource
import org.apache.spark.sql.test.SharedSQLContext

class StructuredStreamingProgressTrackerSuite extends SharedSQLContext {

  test("progress directory is created properly when it does not exist") {
    progressTracker = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())
    assert(fileSystem.exists(progressTracker.progressDirectoryPath))
  }

  test("progress directory is created properly when it exists") {
    fileSystem.mkdirs(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName)))
    progressTracker = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())
    assert(fileSystem.exists(progressTracker.progressDirectoryPath))
  }

  test("temp progress is not cleaned up when partial temp progress exists") {

    val tempPath = new Path(PathTools.progressTempDirPathStr(progressRootPath.toString, appName))
    fileSystem.mkdirs(tempPath)

    val streamId = EventHubsSource.streamIdGenerator.get()

    var tempFilePath = new Path(PathTools.progressTempFileStr(tempPath.toString,
      streamId, eventhubsSource1.uid, eventhubsNamedPartitions("ns1").head, unixTimestamp))
    fileSystem.create(tempFilePath)

    tempFilePath = new Path(PathTools.progressTempFileStr(tempPath.toString,
      streamId, eventhubsSource1.uid, eventhubsNamedPartitions("ns1").tail.head, unixTimestamp))
    fileSystem.create(tempFilePath)

    val filesBefore = fileSystem.listStatus(tempPath)
    assert(filesBefore.size === 2)

    progressTracker = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())

    val filesAfter = fileSystem.listStatus(tempPath)
    assert(filesAfter.size === 2)

  }

  test("incomplete progress will not be discarded") {

    // Register two eventhubs connectors to structured streaming progress tracker

    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource1.uid -> eventhubsSource1
    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource2.uid -> eventhubsSource2

    // Progress record of all partitions of eventhubsSource1 are updated

    val eventHubsSourceStreamId1 = EventHubsSource.streamIdGenerator.get()

    var progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)

    progressWriter.write(Instant.now.getEpochSecond, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)

    progressWriter.write(Instant.now.getEpochSecond, 10L, 10L)

    // Progress records of all partitions of eventhubsSource2 are not updated

    val eventHubsSourceStreamId2 = EventHubsSource.streamIdGenerator.get();

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)

    progressWriter.write(Instant.now.getEpochSecond, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)

    progressWriter.write(Instant.now.getEpochSecond, 100L, 100L)

    StructuredStreamingProgressTracker.initInstance(eventhubsSource1.uid,
      progressRootPath.toString, appName, new Configuration())
    StructuredStreamingProgressTracker.initInstance(eventhubsSource2.uid,
      progressRootPath.toString, appName, new Configuration())

    var progressTempPath = PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName, eventhubsSource1.uid)

    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId1, eventhubsSource1.uid, eventhubsSource1.connectedInstances.head,
      unixTimestamp))))
    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId1, eventhubsSource1.uid, eventhubsSource1.connectedInstances(1),
      unixTimestamp))))

    progressTempPath = PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName, eventhubsSource2.uid)

    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId2, eventhubsSource2.uid, eventhubsSource2.connectedInstances.head,
      unixTimestamp))))

    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId2, eventhubsSource2.uid, eventhubsSource2.connectedInstances(1),
      unixTimestamp))))

    assert(!fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId2, eventhubsSource2.uid, eventhubsSource2.connectedInstances(2),
      unixTimestamp))))

  }

  test("start from the beginning of the streams when the latest progress file does not exist") {

    // Register the two eventhubs connectors to structured streaming progress tracker

    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource3.uid -> eventhubsSource3
    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource4.uid -> eventhubsSource4

    val progressTracker3 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource3.uid, progressRootPath.toString, appName, new Configuration())
    val progressTracker4 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource4.uid, progressRootPath.toString, appName, new Configuration())

    val eh3Progress = progressTracker3.read(eventhubsSource3.uid, unixTimestamp - 1000L,
      fallBack = false)
    val eh4Progress = progressTracker4.read(eventhubsSource4.uid, unixTimestamp - 1000L,
      fallBack = false)

    assert(eh3Progress.offsets(eventhubsSource3.connectedInstances.head) === (-1L, -1L))
    assert(eh3Progress.offsets(eventhubsSource3.connectedInstances(1)) === (-1L, -1L))
    assert(eh3Progress.offsets(eventhubsSource3.connectedInstances(2)) === (-1L, -1L))
    assert(eh3Progress.offsets(eventhubsSource3.connectedInstances(3)) === (-1L, -1L))

    assert(eh4Progress.offsets(eventhubsSource4.connectedInstances.head) === (-1L, -1L))
    assert(eh4Progress.offsets(eventhubsSource4.connectedInstances(1)) === (-1L, -1L))
  }

  test("progress tracker can read back last progress correctly") {

    // Register two eventhubs connectors to structured streaming progress tracker

    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource1.uid -> eventhubsSource1
    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource2.uid -> eventhubsSource2

    // Progress record of all partitions of eventhubsSource1 are updated

    val eventHubsSourceStreamId1 = EventHubsSource.streamIdGenerator.get()

    var progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 10L, 10L)

    // Progress records of all partitions of eventhubsSource2 are updated

    val eventHubsSourceStreamId2 = EventHubsSource.streamIdGenerator.get();

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 100L, 100L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(2), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 200L, 200L)


    val progressTracker1 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())
    progressTracker1.commit(
      progressTracker1.collectProgressRecordsForBatch(unixTimestamp, List(eventhubsSource1)),
      unixTimestamp)

    val progressTracker2 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource2.uid, progressRootPath.toString, appName, new Configuration())
    progressTracker2.commit(progressTracker2.collectProgressRecordsForBatch(unixTimestamp,
      List(eventhubsSource2)), unixTimestamp)

    val eh1Progress = progressTracker1.read(eventhubsSource1.uid, unixTimestamp,
      fallBack = false)
    val eh2Progress = progressTracker2.read(eventhubsSource2.uid, unixTimestamp,
      fallBack = false)

    assert(eh1Progress.offsets(eventhubsSource1.connectedInstances.head) === (0L, 0L))
    assert(eh1Progress.offsets(eventhubsSource1.connectedInstances(1)) === (10L, 10L))
    assert(eh2Progress.offsets(eventhubsSource2.connectedInstances.head) === (0L, 0L))
    assert(eh2Progress.offsets(eventhubsSource2.connectedInstances(1)) === (100L, 100L))
    assert(eh2Progress.offsets(eventhubsSource2.connectedInstances(2)) === (200L, 200L))
  }

  test("inconsistent timestamp in the progress tracks can be detected") {

    // Register two eventhubs connectors to structured streaming progress tracker

    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource1.uid -> eventhubsSource1
    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource2.uid -> eventhubsSource2

    // Progress record of all partitions of eventhubsSource1 are updated

    val eventHubsSourceStreamId1 = EventHubsSource.streamIdGenerator.get()

    var progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 10L, 10L)

    // Progress records of all partitions of eventhubsSource2 are not updated

    val eventHubsSourceStreamId2 = EventHubsSource.streamIdGenerator.get();

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 100L, 100L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(2), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp + 1000L, 200L, 200L)

    val progressTracker1 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())
    progressTracker1.commit(progressTracker1.collectProgressRecordsForBatch(unixTimestamp,
      List(eventhubsSource1)), unixTimestamp)

    val progressTracker2 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource2.uid, progressRootPath.toString, appName, new Configuration())

    intercept[IllegalStateException] {
      progressTracker2.commit(progressTracker2.collectProgressRecordsForBatch(unixTimestamp,
        List(eventhubsSource2)), unixTimestamp)
    }
  }

  test("latest offsets can be committed correctly and temp directory is not cleaned") {

    // Register two eventhubs connectors to structured streaming progress tracker

    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource1.uid -> eventhubsSource1
    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource2.uid -> eventhubsSource2

    // Progress record of all partitions of eventhubsSource1 are updated

    val eventHubsSourceStreamId1 = EventHubsSource.streamIdGenerator.get()

    var progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 10L, 10L)

    // Progress records of all partitions of eventhubsSource2 are not updated

    val eventHubsSourceStreamId2 = EventHubsSource.streamIdGenerator.get();

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 100L, 100L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId2,
      eventhubsSource2.uid, eventhubsSource2.connectedInstances(2), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource2.uid)
    progressWriter.write(unixTimestamp, 200L, 200L)


    val progressTracker1 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())
    progressTracker1.commit(progressTracker1.collectProgressRecordsForBatch(
      unixTimestamp, List(eventhubsSource1)), unixTimestamp)

    val progressTracker2 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource2.uid, progressRootPath.toString, appName, new Configuration())
    progressTracker2.commit(progressTracker2.collectProgressRecordsForBatch(
      unixTimestamp, List(eventhubsSource2)), unixTimestamp)

    var progressTempPath = PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName, eventhubsSource1.uid)

    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId1, eventhubsSource1.uid, eventhubsSource1.connectedInstances.head,
      unixTimestamp))))
    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId1, eventhubsSource1.uid, eventhubsSource1.connectedInstances(1),
      unixTimestamp))))

    progressTempPath = PathTools.progressTempDirPathStr(progressRootPath.toString,
      appName, eventhubsSource2.uid)

    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId2, eventhubsSource2.uid, eventhubsSource2.connectedInstances.head,
      unixTimestamp))))
    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId2, eventhubsSource2.uid, eventhubsSource2.connectedInstances(1),
      unixTimestamp))))
    assert(fileSystem.exists(new Path(PathTools.progressTempFileStr(progressTempPath,
      eventHubsSourceStreamId2, eventhubsSource2.uid, eventhubsSource2.connectedInstances(2),
      unixTimestamp))))
  }

  test("locate progress file correctly based on timestamp") {

    // Register one eventhubs connector to structured streaming progress tracker

    StructuredStreamingProgressTracker.registeredConnectors +=
      eventhubsSource1.uid -> eventhubsSource1

    // Progress record of all partitions of eventhubsSource1 are updated

    val eventHubsSourceStreamId1 = EventHubsSource.streamIdGenerator.get()

    // Update progress for unixTimestamp

    var progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 0L, 0L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp, 10L, 10L)

    // Update progress for unixTimestamp + 1000L

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp + 1000L,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp + 1000L, 20L, 20L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp + 1000L,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp + 1000L, 30L, 30L)

    // Update progress for unixTimestamp + 2000L

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances.head, unixTimestamp + 2000L,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp + 2000L, 40L, 40L)

    progressWriter = new ProgressWriter(eventHubsSourceStreamId1,
      eventhubsSource1.uid, eventhubsSource1.connectedInstances(1), unixTimestamp + 2000L,
      new Configuration(), progressRootPath.toString, appName, eventhubsSource1.uid)
    progressWriter.write(unixTimestamp + 2000L, 50L, 50L)

    val progressTracker1 = StructuredStreamingProgressTracker
      .initInstance(eventhubsSource1.uid, progressRootPath.toString, appName, new Configuration())

    progressTracker1.commit(progressTracker1.collectProgressRecordsForBatch(
      unixTimestamp, List(eventhubsSource1)), unixTimestamp)
    progressTracker1.commit(progressTracker1.collectProgressRecordsForBatch(
      unixTimestamp + 1000L, List(eventhubsSource1)), unixTimestamp + 1000L)
    progressTracker1.commit(progressTracker1.collectProgressRecordsForBatch(
      unixTimestamp + 2000L, List(eventhubsSource1)), unixTimestamp + 2000L)

    var eh1Progress = progressTracker1.read(eventhubsSource1.uid, unixTimestamp,
      fallBack = false)

    assert(eh1Progress.offsets(eventhubsSource1.connectedInstances.head) === (0L, 0L))
    assert(eh1Progress.offsets(eventhubsSource1.connectedInstances(1)) === (10L, 10L))

    eh1Progress = progressTracker1.read(eventhubsSource1.uid, unixTimestamp + 1000L,
      fallBack = false)

    assert(eh1Progress.offsets(eventhubsSource1.connectedInstances.head) === (20L, 20L))
    assert(eh1Progress.offsets(eventhubsSource1.connectedInstances(1)) === (30L, 30L))

    val progressFilePath = progressTracker1.pinPointProgressFile(fileSystem, unixTimestamp + 3000L)

    assert(progressFilePath === None)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    init()
  }

  override def afterEach(): Unit = {
    reset()
  }

  protected def init(): Unit = {
    progressRootPath = new Path(Files.createTempDirectory("progress_root").toString)
    fileSystem = progressRootPath.getFileSystem(new Configuration())
    unixTimestamp = Instant.now.getEpochSecond
  }

  protected def reset(): Unit = {
    StructuredStreamingProgressTracker.reset()
    progressTracker = null
  }

  private val appName = "StrutcuredStreamingApp"

  private val eventhubsNamedPartitions = Map("ns1" -> Seq(EventHubNameAndPartition("eh1", 0),
    EventHubNameAndPartition("eh1", 1)),
    "ns2" -> Seq(EventHubNameAndPartition("eh2", 0), EventHubNameAndPartition("eh2", 1),
      EventHubNameAndPartition("eh", 2)),
    "ns3" -> Seq(EventHubNameAndPartition("eh3", 0), EventHubNameAndPartition("eh3", 1),
      EventHubNameAndPartition("eh3", 2), EventHubNameAndPartition("eh3", 3),
      EventHubNameAndPartition("eh2", 0), EventHubNameAndPartition("eh2", 1)))

  private val eventhubsSource1: EventHubsConnector = new EventHubsConnector {
    override def streamId = 0
    override def uid = "ns1_eh1"
    override def connectedInstances : List[EventHubNameAndPartition] =
      eventhubsNamedPartitions("ns1").toList
  }

  private val eventhubsSource2: EventHubsConnector = new EventHubsConnector {
    override def streamId = 0
    override def uid = "ns2_eh2"
    override def connectedInstances  : List[EventHubNameAndPartition] =
      eventhubsNamedPartitions("ns2").toList
  }

  private val eventhubsSource3: EventHubsConnector = new EventHubsConnector {
    override def streamId = 0
    override def uid = "ns3_eh3"
    override def connectedInstances  : List[EventHubNameAndPartition] =
      eventhubsNamedPartitions("ns3").filter(x => x.eventHubName.equals("eh3")).toList
  }

  private val eventhubsSource4: EventHubsConnector = new EventHubsConnector {
    override def streamId = 0
    override def uid = "ns3_eh2"
    override def connectedInstances  : List[EventHubNameAndPartition] =
      eventhubsNamedPartitions("ns3").filter(x => x.eventHubName.equals("eh2")).toList
  }

  private var fileSystem: FileSystem = _
  private var progressRootPath: Path = _
  private var progressTracker: ProgressTrackerBase[_ <: EventHubsConnector] = _
  private var unixTimestamp: Long = _
}
