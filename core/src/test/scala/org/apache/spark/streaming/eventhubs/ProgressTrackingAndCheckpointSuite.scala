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
import org.apache.spark.streaming.eventhubs.checkpoint.{OffsetRecord, ProgressTracker, ProgressTrackingListener}
import org.apache.spark.streaming.eventhubs.utils.FragileEventHubClient
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
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(3000L), Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputBeforeRestart)
    val eventHubDirectDStream = ssc.graph.getInputStreams().filter(
      _.isInstanceOf[EventHubDirectDStream]).head.asInstanceOf[EventHubDirectDStream]
    assert(eventHubDirectDStream.currentOffsetsAndSeqNums ===
      OffsetRecord(Time(2000L), Map(
        EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
        EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
        EventHubNameAndPartition("eh1", 2) -> (3L, 3L))))
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
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      expectedOffsetsAndSeqs = OffsetRecord(Time(3000L),
        Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputBeforeRestart,
      expectedOutputAfterRestart)
  }

  test("test integration of spark checkpoint and progress tracking (reduceByKeyAndWindow)") {
    val input = Seq(
      Seq("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
      Seq("4", "5", "6", "7", "8", "9", "10", "1", "2", "3"),
      Seq("7", "8", "9", "1", "2", "3", "4", "5", "6", "7"))
    val expectedOutputBeforeRestart = Seq(
      Seq("1" -> 1, "2" -> 1, "4" -> 1, "5" -> 1, "7" -> 1, "8" -> 1),
      Seq("1" -> 2, "2" -> 1, "4" -> 2, "5" -> 1, "7" -> 2, "8" -> 1, "3" -> 1, "6" -> 1,
        "9" -> 1),
      Seq("1" -> 1, "2" -> 1, "4" -> 1, "5" -> 1, "7" -> 1, "8" -> 1, "3" -> 2, "6" -> 2,
        "9" -> 2))
    val expectedOutputAfterRestart = Seq(
      Seq("1" -> 1, "2" -> 1, "4" -> 1, "5" -> 1, "7" -> 1, "8" -> 1, "3" -> 2, "6" -> 2,
        "9" -> 2),
      Seq("5" -> 2, "6" -> 1, "9" -> 1, "2" -> 1, "3" -> 1, "7" -> 1, "8" -> 2,
        "10" -> 1, "1" -> 1, "4" -> 1),
      Seq("7" -> 2, "8" -> 1, "10" -> 2, "1" -> 1, "4" -> 1, "5" -> 1, "9" -> 1,
        "2" -> 1, "3" -> 1, "6" -> 1))

    testCheckpointedOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedStartingOffsetsAndSeqs = Map(eventhubNamespace ->
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
        )),
      expectedOffsetsAndSeqs = OffsetRecord(Time(3000L),
        Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.flatMap(eventData => eventData.getProperties.asScala.
          map{case (_, value) => (value, 1)}).
          reduceByKeyAndWindow(_ + _, _ - _, Seconds(2), Seconds(1)),
      expectedOutputBeforeRestart,
      expectedOutputAfterRestart,
      useSetFlag = true)
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
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      expectedOffsetsAndSeqs = OffsetRecord(Time(3000L),
        Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.window(Seconds(2), Seconds(1)).map(
          eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
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
        OffsetRecord(Time(1000L), Map(EventHubNameAndPartition("eh1", 0) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 1) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 2) -> (2L, 2L))
      )),
      expectedStartingOffsetsAndSeqs2 = Map("namespace2" ->
        OffsetRecord(Time(1000L), Map(EventHubNameAndPartition("eh1", 0) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 1) -> (2L, 2L),
          EventHubNameAndPartition("eh1", 2) -> (2L, 2L))
      )),
      operation = (inputDStream1: EventHubDirectDStream, inputDStream2: EventHubDirectDStream) =>
        inputDStream1.flatMap(eventData => eventData.getProperties.asScala).
          join(inputDStream2.flatMap(eventData => eventData.getProperties.asScala)).
          map{case (key, (v1, v2)) => (key, v1.asInstanceOf[Int] + v2.asInstanceOf[Int])},
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
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputBeforeRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(3000L), Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      4000L)

    ssc.stop()
    reset()

    ssc = createContextForCheckpointOperation(batchDuration, checkpointDirectory)

    ssc.scheduler.clock.asInstanceOf[ManualClock].setTime(5000)
    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        OffsetRecord(Time(6000), Map(EventHubNameAndPartition("eh1", 0) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 1) -> (7L, 7L),
          EventHubNameAndPartition("eh1", 2) -> (7L, 7L))
      )),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputAfterRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(7000L), Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 2) -> (9L, 9L))),
      8000L)
  }

  test("recover correctly when checkpoint writing is delayed") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4), Seq(8, 9, 11, 2, 5, 6),
      Seq(10, 11, 3, 4, 7, 8))

    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputBeforeRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(3000L), Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      4000L)

    val currentCheckpointDirectory = ssc.checkpointDir

    // manipulate checkpoint and progress file
    val fs = FileSystem.get(new Configuration)
    fs.delete(new Path(currentCheckpointDirectory + "/checkpoint-3000"), true)
    fs.delete(new Path(currentCheckpointDirectory + "/checkpoint-3000.bk"), true)

    ssc.stop()
    reset()

    ssc = StreamingContext.getOrCreate(currentCheckpointDirectory,
      () => createContextForCheckpointOperation(batchDuration, checkpointDirectory))


    ssc.graph.getInputStreams().filter(_.isInstanceOf[EventHubDirectDStream]).map(
      _.asInstanceOf[EventHubDirectDStream]).head.currentOffsetsAndSeqNums =
      OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (1L, 1L),
        EventHubNameAndPartition("eh1", 1) -> (1L, 1L),
        EventHubNameAndPartition("eh1", 2) -> (1L, 1L)))


    runStreamsWithEventHubInput(ssc,
      expectedOutputAfterRestart.length - 1,
      expectedOutputAfterRestart, useSet = true)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(5000L), Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 2) -> (9L, 9L))),
      6000L)
  }

  test("continue processing when the application crash before the last commit finished") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(6, 7, 9, 10, 3, 4), Seq(8, 9, 11, 2, 5, 6), Seq(10, 11, 3, 4, 7, 8))

    testUnaryOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputBeforeRestart)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(3000L), Map(EventHubNameAndPartition("eh1", 0) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 1) -> (5L, 5L),
          EventHubNameAndPartition("eh1", 2) -> (5L, 5L))),
      4000L)

    val currentCheckpointDirectory = ssc.checkpointDir

    ssc.stop()
    reset()

    // simulate commit fail
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(progressRootPath.toString + s"/$appName/progress-3000"), true)

    ssc = StreamingContext.getOrCreate(currentCheckpointDirectory,
      () => createContextForCheckpointOperation(batchDuration, checkpointDirectory))

    assert(ssc.graph.getInputStreams().filter(_.isInstanceOf[EventHubDirectDStream]).map(
      _.asInstanceOf[EventHubDirectDStream]).head.currentOffsetsAndSeqNums ===
      OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
        EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
        EventHubNameAndPartition("eh1", 2) -> (3L, 3L))))

    runStreamsWithEventHubInput(ssc,
      expectedOutputAfterRestart.length - 1,
      expectedOutputAfterRestart, useSet = true)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(5000L), Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 2) -> (9L, 9L))),
      6000L)
  }

  test("progress files are clean up correctly with a fragile rest endpoint") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    // the order of the output should looks like there is no issue, because we reuse the fetched
    // highest offset
    val expectedOutputAfterRestart = Seq(
      Seq(6, 7, 9, 10, 3, 4), Seq(8, 9, 11, 2, 5, 6), Seq(10, 11, 3, 4, 7, 8), Seq(), Seq(), Seq())

    // ugly stuff to make things serializable
    FragileEventHubClient.numBatchesBeforeCrashedEndpoint = 3
    FragileEventHubClient.lastBatchWhenEndpointCrashed = 6
    FragileEventHubClient.latestRecords = Map(
      EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
      EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
      EventHubNameAndPartition("eh1", 2) -> (9L, 9L))

    testFragileStream(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1")
      ),
      expectedOffsetsAndSeqs = Map(eventhubNamespace ->
        OffsetRecord(Time(2000L), Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))
      )),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutput = expectedOutputBeforeRestart)

    val currentCheckpointDirectory = ssc.checkpointDir
    ssc.stop()
    reset()

    ssc = new StreamingContext(currentCheckpointDirectory)

    runStreamsWithEventHubInput(ssc,
      expectedOutputAfterRestart.length - 1,
      expectedOutputAfterRestart, useSet = true)

    testProgressTracker(
      eventhubNamespace,
      expectedOffsetsAndSeqs =
        OffsetRecord(Time(8000L), Map(EventHubNameAndPartition("eh1", 0) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 1) -> (9L, 9L),
          EventHubNameAndPartition("eh1", 2) -> (9L, 9L))),
      9000L)
  }

  test("offset type is saved and recovered correctly from checkpoint") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(4, 5, 7, 8, 10, 2))
    val expectedOutputAfterRestart = Seq(
      Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4), Seq(8, 9, 11, 2, 5, 6),
      Seq(10, 11, 3, 4, 7, 8), Seq())

    testCheckpointedOperation(
      input,
      eventhubsParams = Map[String, Map[String, String]](
        "eh1" -> Map(
          "eventhubs.partition.count" -> "3",
          "eventhubs.maxRate" -> "2",
          "eventhubs.name" -> "eh1",
          "eventhubs.filter.enqueuetime" -> "2000")
      ),
      expectedStartingOffsetsAndSeqs = Map(eventhubNamespace ->
        OffsetRecord(Time(0L), Map(EventHubNameAndPartition("eh1", 0) -> (-1L, -1L),
          EventHubNameAndPartition("eh1", 1) -> (-1L, -1L),
          EventHubNameAndPartition("eh1", 2) -> (-1L, -1L))
        )),
      expectedOffsetsAndSeqs = OffsetRecord(Time(1000L),
        Map(EventHubNameAndPartition("eh1", 0) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 1) -> (3L, 3L),
          EventHubNameAndPartition("eh1", 2) -> (3L, 3L))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutputBeforeRestart,
      expectedOutputAfterRestart)
  }
}
