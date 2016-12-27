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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.streaming.{Duration, Seconds}
import org.apache.spark.streaming.eventhubs.checkpoint.{ProgressTracker, ProgressTrackingListener}

class ProgressTrackingAndCheckpointSuite extends CheckpointAndProgressTrackerTestSuiteBase
  with SharedUtils {

  override def beforeEach(): Unit = {
    progressRootPath = new Path(Files.createTempDirectory("progress_root").toString)
    fs = progressRootPath.getFileSystem(new Configuration())
    ssc = createContextForCheckpointOperation(batchDuration, checkpointDirectory)
    progressListener = ProgressTrackingListener.getInstance(ssc, progressRootPath.toString)
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString, appName,
      new Configuration())
  }

  override def batchDuration: Duration = Seconds(1)

  test("test integration of spark checkpoint and progress tracking (single stream)") {
    val input = Seq(
      Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      Seq(4, 5, 6, 7, 8, 9, 10, 1, 2, 3),
      Seq(7, 8, 9, 1, 2, 3, 4, 5, 6, 7))
    val expectedOutputBeforeRestart = Seq(
      Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))
    val expectedOutputAfterRestart = Seq(
      Seq(6, 7, 9, 10, 3, 4), Seq(8, 9, 11, 2, 5, 6), Seq(10, 11, 3, 4, 7, 8))

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
      expectedOutputAfterRestart,
      Duration(2 * batchDuration.milliseconds)
    )
  }
}
