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
import org.apache.spark.streaming.eventhubs.{EventHubDirectDStream, EventHubNameAndPartition, EventHubsUtils}

class ProgressTrackerSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  private val appName = "dummyapp"
  private val streamId = 0
  private val nameSpace = "eventhubs"

  var progressTracker: ProgressTracker = _
  var progressRootPath: Path = _
  var fs: FileSystem = _

  private def createDirectStreams(
      ssc: StreamingContext,
      eventHubNamespace: String,
      checkpointDir: String,
      eventParams: Predef.Map[String, Predef.Map[String, String]]): EventHubDirectDStream = {
    ssc.addStreamingListener(new CheckpointListener(checkpointDir, ssc))
    val newStream = new EventHubDirectDStream(ssc, eventHubNamespace, checkpointDir, eventParams)
    newStream
  }

  before {
    progressRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    fs = progressRootPath.getFileSystem(new Configuration())
  }

  after {
    EventHubDirectDStream.destory()
    ProgressTracker.destory()
    progressTracker = null
  }

  test("progress temp directory is created properly when progress and progress temp" +
    " directory do not exist") {
    progressTracker = ProgressTracker.getInstance(
      progressRootPath.toString + "/progress", appName, new Configuration())
    assert(fs.exists(progressTracker.progressDirPath))
    assert(fs.exists(progressTracker.progressTempDirPath))
  }

  test("progress temp directory is created properly when progress exists while progress" +
    " temp does not") {
    fs.mkdirs(new Path(PathTools.tempProgressDirStr(progressRootPath.toString + "/progress",
      appName)))
    progressTracker = ProgressTracker.getInstance(
      progressRootPath.toString + "/progress", appName, new Configuration())
    assert(fs.exists(progressTracker.progressTempDirPath))
    assert(fs.exists(progressTracker.progressDirPath))
  }

  test("temp progress is cleaned up when a potentially partial temp progress exists") {
    val tempPath = new Path(PathTools.tempProgressDirStr(progressRootPath.toString +
      "/progress", appName))
    fs.mkdirs(tempPath)
    fs.create(new Path(tempPath.toString + "/temp_file"))
    val filesBefore = fs.listStatus(tempPath)
    assert(filesBefore.size === 1)
    progressTracker =
      ProgressTracker.getInstance(progressRootPath.toString + "/progress", appName,
        new Configuration())
    assert(fs.exists(progressTracker.progressTempDirPath))
    val filesAfter = fs.listStatus(progressTracker.progressTempDirPath)
    assert(filesAfter.size === 0)
  }

  test("incomplete progress would be discarded") {
    // create direct streams
    val ssc = new StreamingContext(new SparkContext(
      new SparkConf().setAppName(appName).setMaster("local[*]")), Seconds(5))
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
    progressTracker = ProgressTracker.getInstance(progressRootPath.toString + "/progress", appName,
      new Configuration())
    assert(!fs.exists(new Path(progressPath.toString + "/progress-2000")))
    assert(fs.exists(new Path(progressPath.toString + "/progress-1000")))
    ssc.stop()
  }

  test("health progress file is kept") {
    // create direct streams
    val ssc = new StreamingContext(new SparkContext(
      new SparkConf().setAppName(appName).setMaster("local[*]")), Seconds(5))
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
    progressTracker = ProgressTracker.getInstance(progressRootPath.toString + "/progress", appName,
      new Configuration())
    assert(fs.exists(new Path(progressPath.toString + "/progress-1000")))
    ssc.stop()
  }

  test("start from the beginning of the streams when the latest progress file does not exist") {
    val ssc = new StreamingContext(new SparkContext(
      new SparkConf().setAppName(appName).setMaster("local[*]")), Seconds(5))
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
    progressTracker = ProgressTracker.getInstance(progressRootPath.toString + "/progress", appName,
      new Configuration())
    val ehMap = progressTracker.read("namespace2", dStream1.id, 1000L)
    assert(ehMap(EventHubNameAndPartition("eh11", 0)) === (-1, 0))
    assert(ehMap(EventHubNameAndPartition("eh12", 0)) === (-1, 0))
    assert(ehMap(EventHubNameAndPartition("eh12", 1)) === (-1, 0))
    assert(ehMap(EventHubNameAndPartition("eh13", 0)) === (-1, 0))
    assert(ehMap(EventHubNameAndPartition("eh13", 1)) === (-1, 0))
    assert(ehMap(EventHubNameAndPartition("eh13", 2)) === (-1, 0))
    val ehMap1 = progressTracker.read("namespace1", dStream.id, 1000L)
    assert(ehMap1(EventHubNameAndPartition("eh1", 0)) === (-1, 0))
    assert(ehMap1(EventHubNameAndPartition("eh2", 0)) === (-1, 0))
    assert(ehMap1(EventHubNameAndPartition("eh2", 1)) === (-1, 0))
    assert(ehMap1(EventHubNameAndPartition("eh3", 0)) === (-1, 0))
    assert(ehMap1(EventHubNameAndPartition("eh3", 1)) === (-1, 0))
    assert(ehMap1(EventHubNameAndPartition("eh3", 2)) === (-1, 0))
    ssc.stop()
  }

  test("latest file can be located correctly") {

  }

  test("the progress tracks can be read correctly") {
    val ssc = new StreamingContext(new SparkContext(
      new SparkConf().setAppName(appName).setMaster("local[*]")), Seconds(5))
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
    progressTracker = ProgressTracker.getInstance(progressRootPath.toString + "/progress", appName,
      new Configuration())
    val ehMap = progressTracker.read("namespace2", dStream1.id, 1000L)
    assert(ehMap(EventHubNameAndPartition("eh11", 0)) === (1, 2))
    assert(ehMap(EventHubNameAndPartition("eh12", 0)) === (2, 3))
    assert(ehMap(EventHubNameAndPartition("eh12", 1)) === (3, 4))
    assert(ehMap(EventHubNameAndPartition("eh13", 0)) === (4, 5))
    assert(ehMap(EventHubNameAndPartition("eh13", 1)) === (5, 6))
    assert(ehMap(EventHubNameAndPartition("eh13", 2)) === (6, 7))
    val ehMap1 = progressTracker.read("namespace1", dStream.id, 1000L)
    assert(ehMap1(EventHubNameAndPartition("eh1", 0)) === (0, 1))
    assert(ehMap1(EventHubNameAndPartition("eh2", 0)) === (0, 2))
    assert(ehMap1(EventHubNameAndPartition("eh2", 1)) === (0, 3))
    assert(ehMap1(EventHubNameAndPartition("eh3", 0)) === (0, 4))
    assert(ehMap1(EventHubNameAndPartition("eh3", 1)) === (0, 5))
    assert(ehMap1(EventHubNameAndPartition("eh3", 2)) === (0, 6))
    ssc.stop()
  }

  /*
  test("checkpoint directory can be constructed correctly") {
    progressTracker = ProgressTracker.getInstance(
      progressRootPath.toString + "/checkpoint", appName, new Configuration())
    for (time <- List(2000L, 3000L, 4000L)) {
      for (ehName <- ("eh1", "eh2", "eh3")) {
        val writer = new ProgressWriter(progressRootPath.toString + "/progress",
          appName, streamId, nameSpace, EventHubNameAndPartition(ehName, 1), new Configuration())
        writer.write(time, time * 2, time * 3)
      }

    }
    // tests
    assert(fs.exists(progressTracker.checkpointDirPath))
    assert(fs.exists(progressTracker.checkpointTempDirPath))
    assert(fs.listStatus(offsetStore.checkpointTempDirPath).isEmpty)
    assert(fs.exists(offsetStore.checkpointBackupDirPath))
    assert(fs.exists(new Path(offsetStore.checkpointBackupDirPath.toString +
      s"/$nameSpace-2000")))
    assert(fs.exists(new Path(offsetStore.checkpointBackupDirPath.toString +
      s"/$nameSpace-3000")))
  }
  */
  /*
  test("inconsistent checkpoint file is detected") {
    offsetStore = OffsetStoreDirectStreaming.newInstance(
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
  */
}
