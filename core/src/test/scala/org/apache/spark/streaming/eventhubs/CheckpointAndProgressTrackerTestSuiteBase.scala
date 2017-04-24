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

import scala.reflect.ClassTag

import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.eventhubs.checkpoint.{OffsetRecord, ProgressTracker}
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

  private def validateTempFileCleanup(
      numNonExistBatch: Int,
      numBatches: Int,
      expectedFileNum: Int): Unit = {
    assert(fs.listStatus(new Path(progressRootPath.toString + s"/${appName}_temp"),
      new PathFilter {
        override def accept(path: Path): Boolean = {
          ProgressTracker.getInstance.fromPathToTimestamp(path) < 1000 * numNonExistBatch
        }
      }).length == 0)
    // we do not consider APIs like take() here
    assert(fs.listStatus(new Path(progressRootPath.toString + s"/${appName}_temp"),
      new PathFilter {
        override def accept(path: Path): Boolean = {
          ProgressTracker.getInstance.fromPathToTimestamp(path) == 1000 * numBatches
        }
      }).length == expectedFileNum)
  }

  // NOTE: due to SPARK-19280 (https://issues.apache.org/jira/browse/SPARK-19280)
  // we have to disable cleanup thread
  private def validateProgressFileCleanup(numNonExistBatch: Int, numBatches: Int): Unit = {
    // test cleanup of progress files
    // this test is tricky: because the offset committing and checkpoint data cleanup are performed
    // in two different threads (listener and eventloop thread of job generator respectively), there
    // are two possible cases:
    // a. batch t finishes, commits and cleanup all progress files which is no later than t except
    // the closest one
    // b. batch t finishes, cleanup all progress files which is no later than t except the closest
    // one and commits
    // what is determined is that after batch t finishes, progress-t shall exist and all progress
    // files earlier than batch t - 1 shall not exit,
    // the existence of progress-(t-1) depends on the scheduling of threads

    // check progress directory
    /*
    val fs = progressRootPath.getFileSystem(new Configuration)
    for (i <- 0 until numNonExistBatch) {
      assert(!fs.exists(new Path(progressRootPath.toString + s"/$appName/progress-" +
        s"${(i + 1) * 1000}")))
    }
    assert(fs.exists(new Path(progressRootPath.toString + s"/$appName/" +
      s"progress-${numBatches * 1000}")))
    */

  }


  protected def testCheckpointedOperation[U: ClassTag, V: ClassTag, W: ClassTag](
       input1: Seq[Seq[U]],
       input2: Seq[Seq[V]],
       eventhubsParams1: Map[String, Map[String, String]],
       eventhubsParams2: Map[String, Map[String, String]],
       expectedStartingOffsetsAndSeqs1: Map[String, OffsetRecord],
       expectedStartingOffsetsAndSeqs2: Map[String, OffsetRecord],
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

    validateProgressFileCleanup(expectedOutputBeforeRestart.length - 2,
      expectedOutputBeforeRestart.length)
    validateTempFileCleanup(expectedOutputBeforeRestart.length - 1,
      expectedOutputBeforeRestart.length,
      expectedStartingOffsetsAndSeqs1.values.flatMap(_.offsets).size +
        expectedStartingOffsetsAndSeqs2.values.flatMap(_.offsets).size)

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

    // test cleanup of progress files
    validateProgressFileCleanup(
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 3,
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 1)
    validateTempFileCleanup(
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 2,
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 1,
      expectedStartingOffsetsAndSeqs1.values.flatMap(_.offsets).size +
        expectedStartingOffsetsAndSeqs2.values.flatMap(_.offsets).size)
  }

  protected def runStopAndRecover[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]],
      expectedStartingOffsetsAndSeqs: Map[String, OffsetRecord],
      expectedOffsetsAndSeqs: OffsetRecord,
      operation: EventHubDirectDStream => DStream[V],
      expectedOutputBeforeRestart: Seq[Seq[V]],
      useSetFlag: Boolean = false): Unit = {

    testUnaryOperation(
      input,
      eventhubsParams,
      expectedStartingOffsetsAndSeqs,
      operation,
      expectedOutputBeforeRestart,
      useSet = useSetFlag)
    testProgressTracker(eventhubNamespace, expectedOffsetsAndSeqs, 4000L)

    validateProgressFileCleanup(expectedOutputBeforeRestart.length - 2,
      expectedOutputBeforeRestart.length)
    validateTempFileCleanup(
      expectedOutputBeforeRestart.length - 1,
      expectedOutputBeforeRestart.length,
      expectedOffsetsAndSeqs.offsets.size)

    val currentCheckpointDir = ssc.checkpointDir
    // simulate down
    reset()
    // restart
    ssc = new StreamingContext(currentCheckpointDir)
  }

  protected def testCheckpointedOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]],
      expectedStartingOffsetsAndSeqs: Map[String, OffsetRecord],
      expectedOffsetsAndSeqs: OffsetRecord,
      operation: EventHubDirectDStream => DStream[V],
      expectedOutputBeforeRestart: Seq[Seq[V]],
      expectedOutputAfterRestart: Seq[Seq[V]],
      useSetFlag: Boolean = false) {

    require(ssc.conf.get("spark.streaming.clock") === classOf[ManualClock].getName,
      "Cannot run test without manual clock in the conf")

    runStopAndRecover(input, eventhubsParams, expectedStartingOffsetsAndSeqs,
      expectedOffsetsAndSeqs, operation, expectedOutputBeforeRestart, useSetFlag = useSetFlag)

    // Restart and complete the computation from checkpoint file
    logInfo(
      "\n-------------------------------------------\n" +
        s"        Restarting stream computation at ${ssc.initialCheckpoint.checkpointTime}    " +
        "\n-------------------------------------------\n"
    )

    runStreamsWithEventHubInput(ssc, expectedOutputAfterRestart.length - 1,
      expectedOutputAfterRestart, useSet = useSetFlag)

    validateProgressFileCleanup(
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 3,
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 1)
    validateTempFileCleanup(
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 2,
      expectedOutputBeforeRestart.length + expectedOutputAfterRestart.length - 1,
      expectedOffsetsAndSeqs.offsets.size)
  }
}
