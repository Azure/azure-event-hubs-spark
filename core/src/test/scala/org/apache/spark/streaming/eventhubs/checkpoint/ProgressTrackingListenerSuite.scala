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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.eventhubs.common.{ EventHubsConf, NameAndPartition, OffsetRecord }
import org.apache.spark.eventhubs.common.progress.ProgressWriter
import org.apache.spark.streaming.eventhubs.SharedUtils
import org.apache.spark.streaming.scheduler.OutputOperationInfo
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{
  BatchInfo,
  StreamInputInfo,
  StreamingListenerBatchCompleted
}

class ProgressTrackingListenerSuite extends SharedUtils {

  test("commit offsetsAndSeqNos with a successful micro batch correctly") {
    val batchCompletedEvent = StreamingListenerBatchCompleted(
      BatchInfo(
        Time(1000L),
        Map(0 -> StreamInputInfo(0, 10000)),
        0L,
        None,
        None,
        Map(1 -> OutputOperationInfo(Time(1000L), 1, "output", "", None, None, None))
      ))
    val ehConf = EventHubsConf()
      .setNamespace("eventhubs")
      .setName("eh1")
      .setKeyName("policyname")
      .setKey("policykey")
      .setPartitionCount("2")
      .setStartOfStream(true)
      .setProgressDirectory(progressRootPath.toString)

    val dstream = createDirectStreams(ssc, ehConf)
    dstream.start()
    val progressWriter = new ProgressWriter(streamId,
                                            eventhubNamespace,
                                            NameAndPartition("eh1", 1),
                                            1000L,
                                            new Configuration(),
                                            progressRootPath.toString,
                                            appName)
    progressWriter.write(1000L, 1L, 2L)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    progressListener.onBatchCompleted(batchCompletedEvent)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    assert(fs.exists(new Path(progressTracker.progressDirectoryPath + "/progress-1000")))
    val record = progressTracker
      .asInstanceOf[DirectDStreamProgressTracker]
      .read(eventhubNamespace, 1000L, fallBack = false)
    assert(
      record === OffsetRecord(1000L,
                              Map(NameAndPartition("eh1", 0) -> (-1L, -1L),
                                  NameAndPartition("eh1", 1) -> (1L, 2L))))
  }

  test("do not commit offsetsAndSeqNos when there is a failure in microbatch") {
    val batchCompletedEvent = StreamingListenerBatchCompleted(
      BatchInfo(
        Time(1000L),
        Map(0 -> StreamInputInfo(0, 10000)),
        0L,
        None,
        None,
        Map(
          1 -> OutputOperationInfo(Time(1000L),
                                   1,
                                   "outputWithFailure",
                                   "",
                                   None,
                                   None,
                                   Some("instrumented failure")),
          2 -> OutputOperationInfo(Time(1000L), 2, "correct output", "", None, None, None)
        )
      ))
    // build temp directories
    val progressWriter = new ProgressWriter(streamId,
                                            eventhubNamespace,
                                            NameAndPartition("eh1", 1),
                                            1000L,
                                            new Configuration(),
                                            progressTracker.tempDirectoryPath.toString,
                                            appName)
    progressWriter.write(1000L, 0L, 0L)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    progressListener.onBatchCompleted(batchCompletedEvent)
    assert(fs.exists(progressWriter.tempProgressTrackingPointPath))
    assert(!fs.exists(new Path(progressTracker.progressDirectoryPath + "/progress-1000")))
  }

  test("ProgressTrackingListener is registered correctly") {
    // reset env first
    ProgressTrackingListener.reset(ssc)
    ssc.stop()
    // create new streaming context
    ssc = new StreamingContext(
      new SparkContext(new SparkConf().setAppName(appName).setMaster("local[*]")),
      Seconds(5))

    val ehConf1 = EventHubsConf()
      .setNamespace("namespace1")
      .setName("eh1")
      .setKeyName("policyname")
      .setKey("policykey")
      .setPartitionCount("1")
      .setStartOfStream(true)
      .setProgressDirectory(progressRootPath.toString)
    createDirectStreams(ssc, ehConf1).start()

    val ehConf2 = ehConf1.copy.setNamespace("namespace2").setName("eh11")
    createDirectStreams(ssc, ehConf2).start()

    import scala.collection.JavaConverters._
    assert(
      ssc.scheduler.listenerBus.listeners.asScala
        .count(_.isInstanceOf[ProgressTrackingListener]) === 1)
    assert(DirectDStreamProgressTracker.registeredConnectors.length === 2)
    ssc.stop()
  }
}
