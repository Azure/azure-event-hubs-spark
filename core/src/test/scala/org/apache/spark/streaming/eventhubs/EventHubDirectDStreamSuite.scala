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

import org.apache.spark.eventhubs.common.{ EventHubsConf, NameAndPartition, OffsetRecord }
import org.apache.spark.eventhubs.common.client.{ Client, EventHubsClientWrapper }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Duration, Seconds, Time }
import org.mockito.{ Matchers, Mockito }
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

class EventHubDirectDStreamSuite extends EventHubTestSuiteBase with MockitoSugar with SharedUtils {

  override protected val streamingClock = "org.apache.spark.util.ManualClock"

  override def batchDuration: Duration = Seconds(1)

  val eventhubParameters: Map[String, String] = Map(
    "eventhubs.policyname" -> "policyName",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "eventhubs",
    "eventhubs.name" -> "eh1",
    "eventhubs.partition.count" -> "32",
    "eventhubs.consumergroup" -> "$Default"
  )

  test("interaction among Listener/ProgressTracker/Spark Streaming (single stream)") {
    val input = Seq(Seq(1, 2, 3, 4, 5, 6), Seq(4, 5, 6, 7, 8, 9), Seq(7, 8, 9, 1, 2, 3))
    val expectedOutput = Seq(Seq(2, 3, 5, 6, 8, 9), Seq(4, 5, 7, 8, 10, 2), Seq(6, 7, 9, 10, 3, 4))

    val conf = new EventHubsConf("eventhubs", "eh1", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(2)

    testUnaryOperation(
      input,
      conf,
      expectedOffsetsAndSeqs = Map(
        eventhubNamespace ->
          OffsetRecord(2000L,
                       Map(NameAndPartition("eh1", 0) -> (3L, 3L),
                           NameAndPartition("eh1", 1) -> (3L, 3L),
                           NameAndPartition("eh1", 2) -> (3L, 3L)))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutput
    )
    testProgressTracker(
      eventhubNamespace,
      OffsetRecord(3000L,
                   Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                       NameAndPartition("eh1", 1) -> (5L, 5L),
                       NameAndPartition("eh1", 2) -> (5L, 5L))),
      4000L
    )
  }

  test(
    "interaction among Listener/ProgressTracker/Spark Streaming (single stream +" +
      " windowing function)") {
    val input = Seq(Seq(1, 2, 3, 4, 5, 6), Seq(4, 5, 6, 7, 8, 9), Seq(7, 8, 9, 1, 2, 3))
    val expectedOutput = Seq(Seq(2, 3, 5, 6, 8, 9),
                             Seq(2, 3, 5, 6, 8, 9, 4, 5, 7, 8, 10, 2),
                             Seq(4, 5, 7, 8, 10, 2, 6, 7, 9, 10, 3, 4))

    val conf = new EventHubsConf("eventhubs", "eh1", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(2)

    testUnaryOperation(
      input,
      conf,
      expectedOffsetsAndSeqs = Map(
        eventhubNamespace ->
          OffsetRecord(2000L,
                       Map(NameAndPartition("eh1", 0) -> (3L, 3L),
                           NameAndPartition("eh1", 1) -> (3L, 3L),
                           NameAndPartition("eh1", 2) -> (3L, 3L)))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream
          .window(Seconds(2), Seconds(1))
          .map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutput
    )
    testProgressTracker(
      eventhubNamespace,
      OffsetRecord(3000L,
                   Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                       NameAndPartition("eh1", 1) -> (5L, 5L),
                       NameAndPartition("eh1", 2) -> (5L, 5L))),
      4000L
    )
  }

  test("interaction among Listener/ProgressTracker/Spark Streaming (multi-streams join)") {
    import scala.collection.JavaConverters._
    val input1 = Seq(Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 6),
                     Seq("g" -> 4, "h" -> 5, "i" -> 6, "j" -> 7, "k" -> 8, "l" -> 9),
                     Seq("m" -> 7, "n" -> 8, "o" -> 9, "p" -> 1, "q" -> 2, "r" -> 3))
    val input2 = Seq(Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 6),
                     Seq("g" -> 4, "h" -> 5, "i" -> 6, "j" -> 7, "k" -> 8, "l" -> 9),
                     Seq("m" -> 7, "n" -> 8, "o" -> 9, "p" -> 1, "q" -> 2, "r" -> 3))
    val expectedOutput = Seq(Seq("a" -> 2,
                                 "b" -> 4,
                                 "c" -> 6,
                                 "g" -> 8,
                                 "h" -> 10,
                                 "i" -> 12,
                                 "m" -> 14,
                                 "n" -> 16,
                                 "o" -> 18),
                             Seq("d" -> 8,
                                 "e" -> 10,
                                 "f" -> 12,
                                 "j" -> 14,
                                 "k" -> 16,
                                 "l" -> 18,
                                 "p" -> 2,
                                 "q" -> 4,
                                 "r" -> 6))

    val ehConf1 = new EventHubsConf("namespace1", "eh11", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(3)
    val ehConf2 = new EventHubsConf("namespace2", "eh21", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(3)
    testBinaryOperation(
      input1,
      input2,
      ehConf1,
      ehConf2,
      expectedOffsetsAndSeqs1 = Map(
        "namespace1" ->
          OffsetRecord(1000L,
                       Map(NameAndPartition("eh11", 0) -> (2L, 2L),
                           NameAndPartition("eh11", 1) -> (2L, 2L),
                           NameAndPartition("eh11", 2) -> (2L, 2L)))),
      expectedOffsetsAndSeqs2 = Map(
        "namespace2" ->
          OffsetRecord(1000L,
                       Map(NameAndPartition("eh21", 0) -> (2L, 2L),
                           NameAndPartition("eh21", 1) -> (2L, 2L),
                           NameAndPartition("eh21", 2) -> (2L, 2L)))),
      // join and sum up the value
      operation = (inputDStream1: EventHubDirectDStream, inputDStream2: EventHubDirectDStream) =>
        inputDStream1
          .flatMap(eventData => eventData.getProperties.asScala)
          .join(inputDStream2.flatMap(eventData => eventData.getProperties.asScala))
          .map { case (key, (v1, v2)) => (key, v1.asInstanceOf[Int] + v2.asInstanceOf[Int]) },
      expectedOutput
    )
    testProgressTracker(
      "namespace1",
      OffsetRecord(2000L,
                   Map(NameAndPartition("eh11", 0) -> (5L, 5L),
                       NameAndPartition("eh11", 1) -> (5L, 5L),
                       NameAndPartition("eh11", 2) -> (5L, 5L))),
      3000L
    )
    testProgressTracker(
      "namespace2",
      OffsetRecord(2000L,
                   Map(NameAndPartition("eh21", 0) -> (5L, 5L),
                       NameAndPartition("eh21", 1) -> (5L, 5L),
                       NameAndPartition("eh21", 2) -> (5L, 5L))),
      3000L
    )
  }

  test("update offset correctly when RDD operation only involves some of the partitions") {
    val input = Seq(Seq(1, 2, 3, 4, 5, 6), Seq(4, 5, 6, 7, 8, 9), Seq(7, 8, 9, 1, 2, 3))
    val expectedOutput = Seq(Seq(2), Seq(4), Seq(6))

    val ehConf = new EventHubsConf("eventhubs", "eh1", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(2)

    testUnaryOperation(
      input,
      ehConf,
      expectedOffsetsAndSeqs = Map(
        eventhubNamespace ->
          OffsetRecord(2000L,
                       Map(NameAndPartition("eh1", 0) -> (3L, 3L),
                           NameAndPartition("eh1", 1) -> (-1L, -1L),
                           NameAndPartition("eh1", 2) -> (-1L, -1L)))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutput,
      rddOperation = Some((rdd: RDD[Int], _: Time) => {
        Array(rdd.take(1).toSeq)
      })
    )

    testProgressTracker(
      eventhubNamespace,
      OffsetRecord(3000L,
                   Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                       NameAndPartition("eh1", 1) -> (-1L, -1L),
                       NameAndPartition("eh1", 2) -> (-1L, -1L))),
      4000L
    )
  }

  // TODO revisit test after client consolidation is complete. there's an issue in testFluctuatedStream.
  ignore("continue stream correctly when there is fluctuation") {
    val input = Seq(Seq(1, 2, 3, 4, 5, 6), Seq(4, 5, 6, 7, 8, 9), Seq(7, 8, 9, 1, 2, 3))
    val expectedOutput = Seq(Seq(2, 3, 5, 6, 8, 9),
                             Seq(4, 5, 7, 8, 10, 2),
                             Seq(),
                             Seq(),
                             Seq(),
                             Seq(6, 7, 9, 10, 3, 4))
    val ehConf = new EventHubsConf("eventhubs", "eh1", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(2)

    testFluctuatedStream(
      input,
      ehConf,
      expectedOffsetsAndSeqs = Map(
        eventhubNamespace ->
          OffsetRecord(5000L,
                       Map(NameAndPartition("eh1", 0) -> (3L, 3L),
                           NameAndPartition("eh1", 1) -> (3L, 3L),
                           NameAndPartition("eh1", 2) -> (3L, 3L)))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutput,
      messagesBeforeEmpty = 4,
      numBatchesBeforeNewData = 5
    )
    testProgressTracker(
      eventhubNamespace,
      OffsetRecord(6000L,
                   Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                       NameAndPartition("eh1", 1) -> (5L, 5L),
                       NameAndPartition("eh1", 2) -> (5L, 5L))),
      7000L
    )
  }

  test("filter messages for enqueueTime correctly") {
    val input = Seq(Seq(1, 2, 3, 4, 5, 6), Seq(4, 5, 6, 7, 8, 9), Seq(7, 8, 9, 1, 2, 3))
    val expectedOutput = Seq(Seq(5, 6, 8, 9, 2, 3), Seq(7, 10, 4), Seq())
    val ehConf = new EventHubsConf("eventhubs", "eh1", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(2)

    testUnaryOperation(
      input,
      ehConf,
      expectedOffsetsAndSeqs = Map(
        eventhubNamespace ->
          OffsetRecord(2000L,
                       Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                           NameAndPartition("eh1", 1) -> (5L, 5L),
                           NameAndPartition("eh1", 2) -> (5L, 5L)))),
      operation = (inputDStream: EventHubDirectDStream) =>
        inputDStream.map(eventData => eventData.getProperties.get("output").asInstanceOf[Int] + 1),
      expectedOutput
    )
    testProgressTracker(
      eventhubNamespace,
      OffsetRecord(3000L,
                   Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                       NameAndPartition("eh1", 1) -> (5L, 5L),
                       NameAndPartition("eh1", 2) -> (5L, 5L))),
      4000L
    )
  }

  test("pass-in enqueuetime is not allowed to be later than the highest enqueuetime") {
    val input = Seq(Seq(1, 2, 3, 4, 5, 6), Seq(4, 5, 6, 7, 8, 9), Seq(7, 8, 9, 1, 2, 3))
    val expectedOutput = Seq(Seq(5, 6, 8, 9, 2, 3), Seq(7, 10, 4), Seq())
    val ehConf = new EventHubsConf("eventhubs", "eh1", "policyname", "policykey", "3", "dir")
      .setMaxRatePerPartition(2)
      .setStartEnqueueTimes(10000)
    intercept[IllegalArgumentException] {
      testUnaryOperation(
        input,
        ehConf,
        expectedOffsetsAndSeqs = Map(
          eventhubNamespace ->
            OffsetRecord(2000L,
                         Map(NameAndPartition("eh1", 0) -> (5L, 5L),
                             NameAndPartition("eh1", 1) -> (5L, 5L),
                             NameAndPartition("eh1", 2) -> (5L, 5L)))),
        operation = (inputDStream: EventHubDirectDStream) =>
          inputDStream.map(eventData =>
            eventData.getProperties.get("output").asInstanceOf[Int] + 1),
        expectedOutput
      )
    }
  }
}
