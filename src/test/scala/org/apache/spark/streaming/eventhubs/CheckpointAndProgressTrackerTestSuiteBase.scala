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
import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.ManualClock

/**
 * A trait of that can be mixed in to get methods for testing DStream operations under
 * DStream checkpointing. Note that the implementations of this trait has to implement
 * the `setupCheckpointOperation`
 */
trait CheckpointAndProgressTrackerTestSuiteBase extends EventHubTestSuiteBase { self: SharedUtils =>

  protected def createContextForCheckpointOperation(
      batchDuration: Duration, checkpointDirectory: String): StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    conf.set("spark.streaming.clock", classOf[ManualClock].getName())
    val ssc = new StreamingContext(SparkContext.getOrCreate(conf), batchDuration)
    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  protected def getTestOutputStream[V: ClassTag](streams: Array[DStream[_]]):
      TestEventHubOutputStream[V] = {
    streams.collect {
      case ds: TestEventHubOutputStream[V @unchecked] => ds
    }.head
  }

  protected def testCheckpointedOperation[U: ClassTag, V: ClassTag, W: ClassTag](
       input1: Seq[Seq[U]],
       input2: Seq[Seq[V]],
       eventhubsParams1: Map[String, Map[String, String]],
       eventhubsParams2: Map[String, Map[String, String]],
       expectedStartingOffsetsAndSeqs1: Map[String, Map[EventHubNameAndPartition, (Long, Long)]],
       expectedStartingOffsetsAndSeqs2: Map[String, Map[EventHubNameAndPartition, (Long, Long)]],
       operation: (EventHubDirectDStream, EventHubDirectDStream) => DStream[W],
       expectedOutputBeforeRestart: Seq[Seq[W]],
       expectedOutputAfterRestart: Seq[Seq[W]]) {

    require(ssc.conf.get("spark.streaming.clock") === classOf[ManualClock].getName,
      "Cannot run test without manual clock in the conf")

    testBinaryOperation(
      input1,
      input2,
      eventhubsParams1,
      eventhubsParams2,
      expectedStartingOffsetsAndSeqs1,
      expectedStartingOffsetsAndSeqs2,
      operation,
      expectedOutputBeforeRestart)

    val currentCheckpointDir = ssc.checkpointDir

    // simulate down
    reset()

    // restart
    ssc = new StreamingContext(currentCheckpointDir)

    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
        s"        Restarting stream computation at ${ssc.initialCheckpoint.checkpointTime}    " +
        "\n-------------------------------------------\n"
    )

    runStreamsWithEventHubInput(ssc,
      expectedOutputAfterRestart.length - 1,
      expectedOutputAfterRestart, useSet = true)
  }

  protected def runStopAndRecover[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]],
      expectedStartingOffsetsAndSeqs: Map[String, Map[EventHubNameAndPartition, (Long, Long)]],
      expectedOffsetsAndSeqs: Map[EventHubNameAndPartition, (Long, Long)],
      operation: EventHubDirectDStream => DStream[V],
      expectedOutputBeforeRestart: Seq[Seq[V]]): Unit = {
    testUnaryOperation(
      input,
      eventhubsParams,
      expectedStartingOffsetsAndSeqs,
      operation,
      expectedOutputBeforeRestart)
    testProgressTracker(eventhubNamespace, expectedOffsetsAndSeqs, 4000L)

    val currentCheckpointDir = ssc.checkpointDir

    // simulate down
    reset()

    // restart
    ssc = new StreamingContext(currentCheckpointDir)
  }

  protected def testCheckpointedOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]],
      expectedStartingOffsetsAndSeqs: Map[String, Map[EventHubNameAndPartition, (Long, Long)]],
      expectedOffsetsAndSeqs: Map[EventHubNameAndPartition, (Long, Long)],
      operation: EventHubDirectDStream => DStream[V],
      expectedOutputBeforeRestart: Seq[Seq[V]],
      expectedOutputAfterRestart: Seq[Seq[V]]) {

    require(ssc.conf.get("spark.streaming.clock") === classOf[ManualClock].getName,
      "Cannot run test without manual clock in the conf")

    runStopAndRecover(input, eventhubsParams, expectedStartingOffsetsAndSeqs,
      expectedOffsetsAndSeqs, operation, expectedOutputBeforeRestart)

    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
        s"        Restarting stream computation at ${ssc.initialCheckpoint.checkpointTime}    " +
        "\n-------------------------------------------\n"
    )

    runStreamsWithEventHubInput(ssc, expectedOutputAfterRestart.length - 1,
      expectedOutputAfterRestart, useSet = false)
  }
}
