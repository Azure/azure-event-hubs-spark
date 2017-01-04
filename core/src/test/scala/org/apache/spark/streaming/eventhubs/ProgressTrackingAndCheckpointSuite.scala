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

package org.apache.spark.streaming.eventhubs

import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.streaming._
import org.apache.spark.streaming.eventhubs.checkpoint.{ProgressTracker, ProgressTrackingListener}
import org.apache.spark.util.ManualClock

class ProgressTrackingAndCheckpointSuite extends CheckpointAndProgressTrackerTestSuiteBase
  with SharedUtils {

  override def init(): Unit = {
    progressRootPath = new Path(Files.createTempDirectory("progress_root").toString)
    fs = progressRootPath.getFileSystem(new Configuration())
    ssc = createContextForCheckpointOperation(batchDuration, checkpointDirectory)
    progressListener = ProgressTrackingListener.initInstance(ssc, progressRootPath.toString)
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
  }

  override def batchDuration: Duration = Seconds(1)

  test("currentOffset, ProgressTracker and EventHubClient are setup correctly when" +
    " EventHubDirectDStream is recovered") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6),
      Seq(4, 5, 6, 7, 8, 9),
      Seq(7, 8, 9, 1, 2, 3))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    runStopAndRecover(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedStartingOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      ),
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 2) -> (5L, 5L)),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputBeforeRestart)
    val eventHubDirectDStream = ssc.graph.getInputStreams().filter(
      _.isInstanceOf[EventHubDirectDStream]).head.asInstanceOf[EventHubDirectDStream]
    assert(eventHubDirectDStream.currentOffsetsAndSeqNums === Map(
      EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
      EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
      EventHubNameAndPartition("eh1", 2) -> (3L, 3L)))
    assert(ProgressTracker.getInstance != null)
    assert(eventHubDirectDStream.eventHubClient != null)
  }

  test("test integration of spark checkpoint and progress tracking (single stream)") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(6, 7, 9, 10, 3, 4), Seq(8, 9, 11, 2, 5, 6), Seq(10, 11, 3, 4, 7, 8), Seq())

    testCheckpointedOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedStartingOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      ),
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 2) -> (5L, 5L)),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputBeforeRestart,
      expectedOutputAfterRestart)
  }

  test("test integration of spark checkpoint and progress tracking (single stream +" +
    " windowing function)") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(2, 3, 5, 6, 8, 9, 4, 5, 7, 8, 10, 2),
      Seq(4, 5, 7, 8, 10, 2, 6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(4, 5, 7, 8, 10, 2, 6, 7, 9, 10, 3, 4),
      Seq(6, 7, 9, 10, 3, 4, 8, 9, 11, 2, 5, 6),
      Seq(8, 9, 11, 2, 5, 6, 10, 11, 3, 4, 7, 8), Seq(10, 11, 3, 4, 7, 8))

    testCheckpointedOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedStartingOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      ),
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 2) -> (5L, 5L)),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.window(Seconds(2), Seconds(1)).map(
          eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputBeforeRestart,
      expectedOutputAfterRestart)
  }

  test("test integration of spark checkpoint and progress tracking (multi-streams join)") {
    val input1 = Seq(
      Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 6, "g" -> 4, "h" -> 5, "i" -> 6),
      Seq("g" -> 4, "h" -> 5, "i" -> 6, "j" -> 7, "k" -> 8, "l" -> 9, "m" -> 7, "n" -> 8, "o" -> 9),
      Seq("m" -> 7, "n" -> 8, "o" -> 9, "p" -> 1, "q" -> 2, "r" -> 3, "a" -> 1, "b" -> 2, "c" -> 3))
    val input2 = Seq(
      Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 6, "g" -> 4, "h" -> 5, "i" -> 6),
      Seq("g" -> 4, "h" -> 5, "i" -> 6, "j" -> 7, "k" -> 8, "l" -> 9, "m" -> 7, "n" -> 8, "o" -> 9),
      Seq("m" -> 7, "n" -> 8, "o" -> 9, "p" -> 1, "q" -> 2, "r" -> 3, "a" -> 1, "b" -> 2, "c" -> 3))
    val expectedOutputBeforeRestart = Seq(
      Seq("a" -> 2, "b" -> 4, "c" -> 6, "g" -> 8, "h" -> 10, "i" -> 12, "m" -> 14, "n" -> 16,
        "o" -> 18),
      Seq("d" -> 8, "e" -> 10, "f" -> 12, "j" -> 14, "k" -> 16, "l" -> 18, "p" -> 2, "q" -> 4,
        "r" -> 6))
    val expectedOutputAfterRestart = Seq(
      Seq("d" -> 8, "e" -> 10, "f" -> 12, "j" -> 14, "k" -> 16, "l" -> 18, "p" -> 2, "q" -> 4,
        "r" -> 6),
      Seq("g" -> 8, "h" -> 10, "i" -> 12, "m" -> 14, "n" -> 16, "o" -> 18,
        "a" -> 2, "b" -> 4, "c" -> 6), Seq())

    testCheckpointedOperation(
      input1,
      input2,
      eventhubsParams1 = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "3",
          "eventhubs.name" -> "eh1")
      ),
      eventhubsParams2 = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "3",
          "eventhubs.name" -> "eh1")
      ),
      expectedStartingOffsetsAndSeqs1 = Map("namespace1" ->
        Map(EventHubNameAndPartition("eh1", 0) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 1) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 2) -> (2L, 2L))
      ),
      expectedStartingOffsetsAndSeqs2 = Map("namespace2" ->
        Map(EventHubNameAndPartition("eh1", 0) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 1) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 2) -> (2L, 2L))
      ),
      operation = (inputDStream1: EventHubDirectDStream, inputDStream2: EventHubDirectDStream) =>
        inputDStream1.flatMap(eventData => eventData.getProperties.asScala).
          join(inputDStream2.flatMap(eventData => eventData.getProperties.asScala)).
          map{case (key, (v1, v2)) => (key, v1.toInt + v2.toInt)},
      expectedOutputBeforeRestart,
      expectedOutputAfterRestart)
  }


  test("recover from progress after updating code (no checkpoint provided)") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(8, 9, 11, 2, 5, 6), Seq(10, 11, 3, 4, 7, 8))

    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      ),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputBeforeRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 2) -> (5L, 5L)),
      4000L)

    ssc.stop()
    reset()

    ssc = createContextForCheckpointOperation(batchDuration, checkpointDirectory)

    ssc.scheduler.clock.asInstanceOf[ManualClock].setTime(3000)
    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 1) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 2) -> (7L, 7L))
      ),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputAfterRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
        EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
        EventHubNameAndPartition("eh1", 2) -> (9L, 9L)),
      7000L)
  }

  test("progress files are cleaned up correctly and the crashed application can recover from" +
    " the remaining files (single stream)") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(8, 9, 11, 2, 5, 6), Seq(10, 11, 3, 4, 7, 8))

    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      ),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputBeforeRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 2) -> (5L, 5L)),
      4000L)

    ssc.stop()
    reset()

    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-1000")))
    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-2000")))
    assert(fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-3000")))

    ssc = createContextForCheckpointOperation(batchDuration, checkpointDirectory)
    // val fs = FileSystem.get(ssc.sparkContext.hadoopConfiguration)
    // fs.delete(new Path(progressDir + "/progress-3000"), true)

    ssc.scheduler.clock.asInstanceOf[ManualClock].setTime(3000)
    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 1) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 2) -> (7L, 7L))
      ),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputAfterRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
        EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
        EventHubNameAndPartition("eh1", 2) -> (9L, 9L)),
      7000L)

    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-3000")))
    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-4000")))
    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-5000")))
    assert(fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-6000")))
  }

  test("progress files are cleaned up correctly and the crashed application can recover from" +
    " the remaining files (windowing)") {

    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(2, 3, 5, 6, 8, 9, 4, 5, 7, 8, 10, 2),
      Seq(2, 3, 5, 6, 8, 9, 4, 5, 7, 8, 10, 2, 6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9, 4, 5, 7, 8, 10, 2, 6, 7, 9, 10, 3, 4),
      Seq(4, 5, 7, 8, 10, 2, 6, 7, 9, 10, 3, 4, 8, 9, 11, 2, 5, 6),
      Seq(6, 7, 9, 10, 3, 4, 8, 9, 11, 2, 5, 6, 10, 11, 3, 4, 7, 8))

    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      ),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").toInt + 1),
      expectedOutputBeforeRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
        EventHubNameAndPartition("eh1", 2) -> (5L, 5L)),
      4000L)

    ssc.stop()
    reset()

    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-1000")))
    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-2000")))
    assert(fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-3000")))

    ssc = createContextForCheckpointOperation(batchDuration, checkpointDirectory)

    ssc.scheduler.clock.asInstanceOf[ManualClock].setTime(3000)
    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        Map(EventHubNameAndPartition("eh1", 0) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 1) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 2) -> (7L, 7L))
      ),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.window(Seconds(3), Seconds(1)).map(eventData =>
          eventData.getProperties.get("output").toInt + 1),
      expectedOutputAfterRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs = Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
        EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
        EventHubNameAndPartition("eh1", 2) -> (9L, 9L)),
      7000L)

    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-3000")))
    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-4000")))
    assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-5000")))
    assert(fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-6000")))
  }
}
