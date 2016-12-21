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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.eventhubs.{EventHubDirectDStream, EventHubNameAndPartition, EventHubsUtils, SharedUtils}

class ProgressTrackerSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter
  with SharedUtils {

  before {
    progressRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    fs = progressRootPath.getFileSystem(new Configuration())
    ssc = new StreamingContext(new SparkContext(
      new SparkConf().setAppName(appName).setMaster("local[*]")), Seconds(5))
  }

  after {
    ProgressTrackingListener.reset()
    ProgressTracker.reset()
    progressTracker = null
    ssc.stop()
  }

  test("progress temp directory is created properly when progress and progress temp" +
    " directory do not exist") {
    progressTracker = ProgressTracker.getInstance(
      ssc,
      progressRootPath.toString + "/progress", appName, new Configuration())
    assert(fs.exists(progressTracker.progressDirPath))
    assert(fs.exists(progressTracker.progressTempDirPath))
  }

  test("progress temp directory is created properly when progress exists while progress" +
    " temp does not") {
    fs.mkdirs(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString + "/progress",
      appName)))
    progressTracker = ProgressTracker.getInstance(
      ssc,
      progressRootPath.toString + "/progress", appName, new Configuration())
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
    progressTracker =
      ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress", appName,
        new Configuration())
    assert(fs.exists(progressTracker.progressTempDirPath))
    val filesAfter = fs.listStatus(progressTracker.progressTempDirPath)
    assert(filesAfter.size === 0)
  }

  test("incomplete progress would be discarded") {
    // create direct streams
    // generate 6 EventHubAndPartitions
    val dStream =
      createDirectStreams(ssc, "namespace1", progressRootPath.toString,
        Map("eh1" -> Map("eventhubs.partition.count" -> "1"),
          "eh2" -> Map("eventhubs.partition.count" -> "2"),
          "eh3" -> Map("eventhubs.partition.count" -> "3")))
    val progressPath = new Path(PathTools.progressDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressPath)
    val oos = fs.create(new Path(progressPath.toString + "/progress-1000"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh1", 0, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 0, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 1, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 0, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 1, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 2, 0, 0).toString + "\n")
    oos.close()

    val oos2 = fs.create(new Path(progressPath.toString + "/progress-2000"))
    oos2.writeBytes(ProgressRecord(2000L, "namespace1", dStream.id, "eh1", 0, 0, 0).toString + "\n")
    oos2.writeBytes(ProgressRecord(2000L, "namespace1", dStream.id, "eh2", 0, 0, 0).toString + "\n")
    oos2.writeBytes(ProgressRecord(2000L, "namespace1", dStream.id, "eh2", 1, 0, 0).toString + "\n")
    oos2.close()
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    assert(!fs.exists(new Path(progressPath.toString + "/progress-2000")))
    assert(fs.exists(new Path(progressPath.toString + "/progress-1000")))
  }

  test("health progress file is kept") {
    // create direct streams, generate 6 EventHubAndPartitions
    val dStream =
      createDirectStreams(ssc, "namespace1", progressRootPath.toString,
        Map("eh1" -> Map("eventhubs.partition.count" -> "1"),
          "eh2" -> Map("eventhubs.partition.count" -> "2"),
          "eh3" -> Map("eventhubs.partition.count" -> "3")))
    val progressPath = new Path(PathTools.progressDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressPath)
    val oos = fs.create(new Path(progressPath.toString + "/progress-1000"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh1", 0, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 0, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 1, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 0, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 1, 0, 0).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 2, 0, 0).toString + "\n")
    oos.close()
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    assert(fs.exists(new Path(progressPath.toString + "/progress-1000")))
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
    val progressPath = new Path(PathTools.progressDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressPath)
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    val ehMap = progressTracker.read("namespace2", dStream1.id, 1000L)
    assert(ehMap(EventHubNameAndPartition("eh11", 0)) === (-1, -1))
    assert(ehMap(EventHubNameAndPartition("eh12", 0)) === (-1, -1))
    assert(ehMap(EventHubNameAndPartition("eh12", 1)) === (-1, -1))
    assert(ehMap(EventHubNameAndPartition("eh13", 0)) === (-1, -1))
    assert(ehMap(EventHubNameAndPartition("eh13", 1)) === (-1, -1))
    assert(ehMap(EventHubNameAndPartition("eh13", 2)) === (-1, -1))
    val ehMap1 = progressTracker.read("namespace1", dStream.id, 1000L)
    assert(ehMap1(EventHubNameAndPartition("eh1", 0)) === (-1, -1))
    assert(ehMap1(EventHubNameAndPartition("eh2", 0)) === (-1, -1))
    assert(ehMap1(EventHubNameAndPartition("eh2", 1)) === (-1, -1))
    assert(ehMap1(EventHubNameAndPartition("eh3", 0)) === (-1, -1))
    assert(ehMap1(EventHubNameAndPartition("eh3", 1)) === (-1, -1))
    assert(ehMap1(EventHubNameAndPartition("eh3", 2)) === (-1, -1))
  }

  test("the progress tracks can be read correctly") {
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
    val progressPath = new Path(PathTools.progressDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressPath)
    val oos = fs.create(new Path(progressPath.toString + "/progress-1000"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh1", 0, 0, 1).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 0, 0, 2).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 1, 0, 3).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 0, 0, 4).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 1, 0, 5).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 2, 0, 6).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh11", 0, 1, 2).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh12", 0, 2, 3).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh12", 1, 3, 4).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh13", 0, 4, 5).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh13", 1, 5, 6).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh13", 2, 6, 7).toString +
      "\n")
    oos.close()
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    val ehMap = progressTracker.read("namespace2", dStream1.id, 2000L)
    assert(ehMap(EventHubNameAndPartition("eh11", 0)) === (1, 2))
    assert(ehMap(EventHubNameAndPartition("eh12", 0)) === (2, 3))
    assert(ehMap(EventHubNameAndPartition("eh12", 1)) === (3, 4))
    assert(ehMap(EventHubNameAndPartition("eh13", 0)) === (4, 5))
    assert(ehMap(EventHubNameAndPartition("eh13", 1)) === (5, 6))
    assert(ehMap(EventHubNameAndPartition("eh13", 2)) === (6, 7))
    val ehMap1 = progressTracker.read("namespace1", dStream.id, 2000L)
    assert(ehMap1(EventHubNameAndPartition("eh1", 0)) === (0, 1))
    assert(ehMap1(EventHubNameAndPartition("eh2", 0)) === (0, 2))
    assert(ehMap1(EventHubNameAndPartition("eh2", 1)) === (0, 3))
    assert(ehMap1(EventHubNameAndPartition("eh3", 0)) === (0, 4))
    assert(ehMap1(EventHubNameAndPartition("eh3", 1)) === (0, 5))
    assert(ehMap1(EventHubNameAndPartition("eh3", 2)) === (0, 6))
  }

  test("inconsistent timestamp in the progress tracks can be detected") {
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
    val progressPath = new Path(PathTools.progressDirPathStr(progressRootPath.toString +
      "/progress", appName))
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    fs.mkdirs(progressPath)
    val oos = fs.create(new Path(progressPath.toString + "/progress-1000"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh1", 0, 0, 1).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 0, 0, 2).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh2", 1, 0, 3).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 0, 0, 4).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 1, 0, 5).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace1", dStream.id, "eh3", 2, 0, 6).toString + "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh11", 0, 1, 2).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh12", 0, 2, 3).toString +
      "\n")
    oos.writeBytes(ProgressRecord(2000L, "namespace2", dStream1.id, "eh12", 1, 3, 4).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh13", 0, 4, 5).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh13", 1, 5, 6).toString +
      "\n")
    oos.writeBytes(ProgressRecord(1000L, "namespace2", dStream1.id, "eh13", 2, 6, 7).toString +
      "\n")
    oos.close()
    intercept[IllegalStateException] {
      progressTracker.read("namespace2", dStream1.id, 1000L)
    }
  }

  test("snapshot progress tracking records can be read correctly") {
    // generate 6 EventHubAndPartitions
    val progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    val progressTempPath = new Path(PathTools.progressTempDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressTempPath)
    var oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-1"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", 0, "eh1", 0, 0, 1).toString + "\n")
    oos.close()
    oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-2"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", 0, "eh2", 0, 0, 1).toString + "\n")
    oos.close()
    oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-3"))
    oos.writeBytes(ProgressRecord(1000L, "namespace2", 0, "eh1", 0, 10, 20).toString + "\n")
    oos.close()
    oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-4"))
    oos.writeBytes(ProgressRecord(1000L, "namespace2", 0, "eh2", 0, 20, 30).toString + "\n")
    oos.close()
    val s = progressTracker.snapshot()
    assert(s.contains("namespace1"))
    assert(s.contains("namespace2"))
    assert(s("namespace1")(EventHubNameAndPartition("eh1", 0)) === (0, 1))
    assert(s("namespace1")(EventHubNameAndPartition("eh2", 0)) === (0, 1))
    assert(s("namespace2")(EventHubNameAndPartition("eh1", 0)) === (10, 20))
    assert(s("namespace2")(EventHubNameAndPartition("eh2", 0)) === (20, 30))
  }

  test("inconsistent timestamp in temp progress directory can be detected") {
    // generate 6 EventHubAndPartitions
    val progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    val progressTempPath = new Path(PathTools.progressTempDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressTempPath)
    var oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-1"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", 0, "eh1", 0, 0, 1).toString + "\n")
    oos.close()
    oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-2"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", 0, "eh2", 0, 0, 1).toString + "\n")
    oos.close()
    oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-3"))
    oos.writeBytes(ProgressRecord(2000L, "namespace2", 0, "eh1", 0, 10, 20).toString + "\n")
    oos.close()
    oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-4"))
    oos.writeBytes(ProgressRecord(1000L, "namespace2", 0, "eh2", 0, 20, 30).toString + "\n")
    oos.close()
    intercept[IllegalStateException] {
      progressTracker.snapshot()
    }
  }

  test("latest offsets can be committed correctly and temp directory is cleaned") {
    val progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString + "/progress",
      appName, new Configuration())
    val progressTempPath = new Path(PathTools.progressTempDirPathStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(progressTempPath)
    val oos = fs.create(new Path(progressTempPath.toString + "/progress-1000-1"))
    oos.writeBytes(ProgressRecord(1000L, "namespace1", 0, "eh1", 0, 0, 1).toString + "\n")
    oos.close()
    assert(fs.exists(new Path(progressTempPath.toString + "/progress-1000-1")))
    val offsetToCommit = Map(
      ("namespace1", 1) -> Map(
        EventHubNameAndPartition("eh1", 0) -> (0L, 0L),
        EventHubNameAndPartition("eh2", 1) -> (1L, 1L)),
      ("namespace2", 2) -> Map(
        EventHubNameAndPartition("eh1", 3) -> (0L, 0L),
        EventHubNameAndPartition("eh2", 4) -> (1L, 1L)))
    progressTracker.commit(offsetToCommit, 3000L)
    val namespace1Offsets = progressTracker.read("namespace1", 1, 4000L)
    assert(namespace1Offsets === Map(
      EventHubNameAndPartition("eh1", 0) -> (0L, 0L),
      EventHubNameAndPartition("eh2", 1) -> (1L, 1L)))
    val namespace2Offsets = progressTracker.read("namespace2", 2, 4000L)
    assert(namespace2Offsets === Map(
      EventHubNameAndPartition("eh1", 3) -> (0L, 0L),
      EventHubNameAndPartition("eh2", 4) -> (1L, 1L)))
    // test temp directory cleanup
    assert(fs.exists(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString +
      "/progress", appName))))
    assert(fs.listStatus(new Path(PathTools.progressTempDirPathStr(progressRootPath.toString +
      "/progress", appName))).length === 0)
  }
}
