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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.{EventHubDirectDStream, EventHubNameAndPartition, EventHubsUtils}
import org.apache.spark.streaming.scheduler.OutputOperationInfo

// scalastyle:off
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamInputInfo, StreamingListenerBatchCompleted}
// scalastyle:on

class ProgressListenerSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter
  with SharedUtils {

  before {
    progressRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    fs = progressRootPath.getFileSystem(new Configuration())
    ssc = new StreamingContext(new SparkContext(new SparkConf().setAppName(appName).
      setMaster("local[*]")), Seconds(5))
    progressListner = ProgressTrackingListener.getInstance(ssc, progressRootPath.toString)
    progressTracker = ProgressTracker.getInstance(ssc, progressRootPath.toString, appName,
      new Configuration())
  }

  after {
    ProgressTracker.destory()
    progressTracker = null
    progressListner = null
    ssc.stop()
  }

  test("commit offsets with a successful micro batch correctly") {
    val batchCompletedEvent = StreamingListenerBatchCompleted(BatchInfo(
      Time(1000L),
      Map(0 -> StreamInputInfo(0, 10000)),
      0L,
      None,
      None,
      Map(1 -> OutputOperationInfo(Time(1000L), 1, "output", "", None, None, None))
    ))
    val dstream = createDirectStreams(ssc, nameSpace, progressRootPath.toString,
      Map("eh1" -> Map("eventhubs.partition.count" -> "2")))
    val progressWriter = new ProgressWriter(progressRootPath.toString,
      appName, streamId, nameSpace, EventHubNameAndPartition("eh1", 1), new Configuration())
    progressWriter.write(1000L, 1L, 2L)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    progressListner.onBatchCompleted(batchCompletedEvent)
    assert(!fs.exists(progressWriter.tempProgressTrackingPointPath))
    assert(fs.exists(new Path(progressTracker.progressDirPath + "/progress-1000")))
    val record = progressTracker.read(nameSpace, streamId, 1000L)
    assert(record === Map(EventHubNameAndPartition("eh1", 1) -> (1L, 2L)))
  }

  test("do not commit offsets when there is a failure in microbatch") {
    val batchCompletedEvent = StreamingListenerBatchCompleted(BatchInfo(
      Time(1000L),
      Map(0 -> StreamInputInfo(0, 10000)),
      0L,
      None,
      None,
      Map(
        1 -> OutputOperationInfo(Time(1000L), 1, "outputWithFailure", "", None, None,
          Some("instrumented failure")),
        2 -> OutputOperationInfo(Time(1000L), 2, "correct output", "", None, None, None)))
    )
    // build temp directories
    val progressWriter = new ProgressWriter(progressTracker.progressTempDirPath.toString,
      appName, streamId, nameSpace, EventHubNameAndPartition("eh1", 1), new Configuration())
    progressWriter.write(1000L, 0L, 0L)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    progressListner.onBatchCompleted(batchCompletedEvent)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    assert(!fs.exists(new Path(progressTracker.progressDirPath + "/progress-1000")))
  }
}
