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

import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Checkpoint, Seconds, StreamingContext, Time}
import org.apache.spark.streaming.eventhubs.checkpoint.{ProgressTracker, ProgressTrackingListener}
import org.apache.spark.util.Utils


class EventHubDirectDStreamSuite extends SharedUtils with MockitoSugar {

  val eventhubParameters = Map[String, String] (
    "eventhubs.policyname" -> "policyName",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "eventhubs",
    "eventhubs.name" -> "eh1",
    "eventhubs.partition.count" -> "32",
    "eventhubs.consumergroup" -> "$Default"
  )


  test("skip the batch when failed to fetch the latest offset of partitions") {
    val eventHubClientMock = mock[EventHubClient]
    Mockito.when(eventHubClientMock.endPointOfPartition()).thenReturn(None)
    val ehDStream = new EventHubDirectDStream(ssc, eventhubNamespace, progressRootPath.toString,
      Map("eh1" -> eventhubParameters))
    ehDStream.setEventHubClient(eventHubClientMock)
    ssc.scheduler.start()
    assert(ehDStream.compute(Time(1000)).get.count() === 0)
  }

  test("currentOffset are setup correctly when EventHubDirectDStream is deserialized") {
    val ehDStream = new EventHubDirectDStream(ssc, eventhubNamespace, progressRootPath.toString,
      Map("eh1" -> eventhubParameters))
    ehDStream.start()
    ehDStream.currentOffsetsAndSeqNums = Map(EventHubNameAndPartition("ehName1", 1) -> (12L, 21L))
    val cp = Utils.serialize(new Checkpoint(ssc, Time(1000)))
    val deserCp = Utils.deserialize[Checkpoint](cp)
    assert(deserCp.graph.getInputStreams().length === 1)
    val deserEhDStream = deserCp.graph.getInputStreams()(0).asInstanceOf[EventHubDirectDStream]
    deserEhDStream.setContext(ssc)
    assert(deserEhDStream.currentOffsetsAndSeqNums ===
      Map(EventHubNameAndPartition("ehName1", 1) -> (12L, 21L)))
  }

  test("ProgressTracker, ProgressTrackingListener and EventHubClient are registered correctly" +
    " when EventHubDirectDStream is deserialized") {
    import scala.collection.JavaConverters._
    val eventHubClientMock = mock[EventHubClient]
    Mockito.when(eventHubClientMock.endPointOfPartition()).thenReturn(None)
    val ehDStream = new EventHubDirectDStream(ssc, eventhubNamespace, progressRootPath.toString,
      Map("eh1" -> eventhubParameters))
    val ehDStream1 = new EventHubDirectDStream(ssc, eventhubNamespace, progressRootPath.toString,
      Map("eh1" -> eventhubParameters))
    val cp = Utils.serialize(new Checkpoint(ssc, Time(1000)))
    reset()
    ssc = new StreamingContext(new SparkContext(new SparkConf().setAppName(appName).
      setMaster("local[*]")), Seconds(5))
    val deserCp = Utils.deserialize[Checkpoint](cp)
    val allEventhubDStreams = deserCp.graph.getInputStreams().filter(
      _.isInstanceOf[EventHubDirectDStream]).map(_.asInstanceOf[EventHubDirectDStream])
    allEventhubDStreams.foreach(_.setEventHubClient(eventHubClientMock).setContext(ssc))
    allEventhubDStreams.foreach(eh => eh.compute(Time(1000L)))
    assert(ProgressTracker.eventHubDirectDStreams.length == 2)
    assert(ehDStream.eventHubClient != null)
    assert(ehDStream1.eventHubClient != null)
    assert(ssc.scheduler.listenerBus.listeners.asScala.count(
      _.isInstanceOf[ProgressTrackingListener]) === 1)
    assert(ssc.scheduler.listenerBus.listeners.get(0).isInstanceOf[ProgressTrackingListener])
    ssc.stop()
  }
}