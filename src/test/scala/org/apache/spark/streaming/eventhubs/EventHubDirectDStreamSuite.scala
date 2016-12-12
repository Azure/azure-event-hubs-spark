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
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Checkpoint, Duration, StreamingContext, Time}
import org.apache.spark.streaming.eventhubs.checkpoint.{DfsBasedOffsetStore2, OffsetStore}
import org.apache.spark.util.Utils


class EventHubDirectDStreamSuite extends FunSuite with BeforeAndAfter with MockitoSugar {

  var ssc: StreamingContext = _
  val eventhubParameters = Map[String, String] (
    "eventhubs.policyname" -> "policyName",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "eh1",
    "eventhubs.partition.count" -> "32",
    "eventhubs.consumergroup" -> "$Default"
  )

  before {
    ssc = new StreamingContext(new SparkContext(new SparkConf().
      set("spark.master", "local[*]").set("spark.app.name", "EventHubDirectDStreamSuite")),
      Duration(10000))
  }

  after {
    ssc.stop()
  }

  test("skip the batch when detecting the same offset") {
    val offsetStoreMock = mock[DfsBasedOffsetStore2]
    Mockito.when(offsetStoreMock.read()).thenReturn(
      Map(EventHubNameAndPartition("eh1", 1) -> (1L, 1L)))
    val checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    val ehDStream = new EventHubDirectDStream(ssc, "ehs", checkpointRootPath.toString,
      Map("eh1" -> eventhubParameters))
    val tempPath = ehDStream.offsetStore.asInstanceOf[DfsBasedOffsetStore2].checkpointTempDirPath
    val fs = tempPath.getFileSystem(new Configuration())
    fs.mkdirs(tempPath)
    ehDStream.setOffsetStore(offsetStoreMock)
    ehDStream.currentOffsetsAndSeqNums = Map(EventHubNameAndPartition("eh1", 1) -> (1L, 1L))
    ssc.scheduler.start()
    assert(ehDStream.compute(Time(1000)).get.count() === 0)
  }

  test("skip the batch when failed to fetch the latest offset of partitions") {
    val eventHubClientMock = mock[EventHubClient]
    Mockito.when(eventHubClientMock.endPointOfPartition()).thenReturn(None)
    val checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    val ehDStream = new EventHubDirectDStream(ssc, "ehs", checkpointRootPath.toString,
      Map("eh1" -> eventhubParameters))
    val tempPath = ehDStream.offsetStore.asInstanceOf[DfsBasedOffsetStore2].checkpointTempDirPath
    val fs = tempPath.getFileSystem(new Configuration())
    fs.mkdirs(tempPath)
    ehDStream.setEventHubClient(eventHubClientMock)
    ssc.scheduler.start()
    assert(ehDStream.compute(Time(1000)).get.count() === 0)
  }

  test("checkpoint directories are configured correctly when EventHubDirectDStream" +
    " is deserialized") {
    val checkpointRootPath = new Path(Files.createTempDirectory("checkpoint_root").toString)
    val ehDStream = new EventHubDirectDStream(ssc, "ehs", checkpointRootPath.toString,
      Map("eh1" -> eventhubParameters))
    val tempPath = ehDStream.offsetStore.asInstanceOf[DfsBasedOffsetStore2].checkpointTempDirPath
    val fs = tempPath.getFileSystem(new Configuration())
    val cp = Utils.serialize(new Checkpoint(ssc, Time(1000)))
    fs.mkdirs(tempPath)
    fs.create(new Path(tempPath + "/temp_file"))
    val filesBefore = fs.listStatus(tempPath)
    assert(filesBefore.size === 1)
    val deserCp = Utils.deserialize[Checkpoint](cp)
    assert(deserCp.graph.getInputStreams().length === 1)
    val deserEhDStream = deserCp.graph.getInputStreams()(0).asInstanceOf[EventHubDirectDStream]
    deserEhDStream.setContext(ssc)
    assert(deserEhDStream.offsetStore != null)
    assert(fs.exists(tempPath))
    val filesAfter = fs.listStatus(tempPath)
    assert(filesAfter.size === 0)
  }
}
