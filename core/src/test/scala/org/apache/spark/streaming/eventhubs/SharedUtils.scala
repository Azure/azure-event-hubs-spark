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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.checkpoint.{ProgressTracker, ProgressTrackingListener}

private[eventhubs] trait SharedUtils extends FunSuite with BeforeAndAfterEach {

  val appName = "dummyapp"
  val streamId = 0
  val eventhubNamespace = "eventhubs"

  var fs: FileSystem = _
  var progressRootPath: Path = _
  var progressListener: ProgressTrackingListener = _
  var ssc: StreamingContext = _
  var progressTracker: ProgressTracker = _

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
    ssc = new StreamingContext(new SparkContext(new SparkConf().setAppName(appName).
      setMaster("local[*]").set("spark.streaming.clock", streamingClock)).setLogLevel("ERROR"),
      batchDuration)
    progressListener = ProgressTrackingListener.initInstance(ssc, progressRootPath.toString)
    progressTracker = ProgressTracker.initInstance(progressRootPath.toString, appName,
      new Configuration())
  }

  protected def reset(): Unit = {
    ProgressTracker.reset()
    progressTracker = null
    progressListener = null
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
