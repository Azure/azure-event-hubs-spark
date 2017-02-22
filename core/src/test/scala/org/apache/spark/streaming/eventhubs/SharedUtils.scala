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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.eventhubscommon.EventHubsConnector
import org.apache.spark.eventhubscommon.progress.ProgressTrackerBase
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.checkpoint.ProgressTrackingListener

private[eventhubs] trait SharedUtils extends FunSuite with BeforeAndAfterEach {

  val appName = "dummyapp"
  val streamId = 0
  val eventhubNamespace = "eventhubs"

  var fs: FileSystem = _
  var progressRootPath: Path = _
  var progressListener: ProgressTrackingListener = _
  var ssc: StreamingContext = _
  var progressTracker: ProgressTrackerBase[_ <: EventHubsConnector] = _

  protected val streamingClock = "org.apache.spark.util.SystemClock"

  override def beforeEach(): Unit = {
    init()
  }

  override def afterEach(): Unit = {
    reset()
  }

  def batchDuration: Duration = Seconds(5)

  protected def init(): Unit = {
    progressRootPath = new Path(Files.createTempDirectory("progress_root").toString)
    fs = progressRootPath.getFileSystem(new Configuration())
    val sparkContext = new SparkContext(new SparkConf().setAppName(appName).
      setMaster("local[*]").set("spark.streaming.clock", streamingClock))
    sparkContext.setLogLevel("INFO")
    ssc = new StreamingContext(sparkContext, batchDuration)
    progressListener = ProgressTrackingListener.initInstance(ssc, progressRootPath.toString)
    progressTracker = ProgressTrackerBase.initInstance(progressRootPath.toString, appName,
      new Configuration(), "directDStream")
  }

  protected def reset(): Unit = {
    ProgressTrackerBase.reset()
    progressTracker = null
    progressListener = null
    EventHubDirectDStream.lastCleanupTime = -1
    ProgressTrackingListener.reset(ssc)
    ssc.stop()
  }

  protected def createDirectStreams(
      ssc: StreamingContext,
      eventHubNamespace: String,
      progressDir: String,
      eventParams: Predef.Map[String, Predef.Map[String, String]]): EventHubDirectDStream = {
    val newStream = new EventHubDirectDStream(ssc, eventHubNamespace, progressDir, eventParams)
    newStream
  }
}
