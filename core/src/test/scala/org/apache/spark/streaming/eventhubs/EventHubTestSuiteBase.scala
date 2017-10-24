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

import java.io.{ IOException, ObjectInputStream }
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.eventhubs.common.{ NameAndPartition, OffsetRecord }
import org.apache.spark.eventhubs.common.utils.{
  EventHubsTestUtilities,
  FluctuatedEventHubClient,
  SimulatedEventHubs,
  TestEventHubsClient
}

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, ForEachDStream }
import org.apache.spark.streaming.eventhubs.checkpoint.DirectDStreamProgressTracker
import org.apache.spark.util.{ ManualClock, Utils }

private[eventhubs] class TestEventHubOutputStream[T: ClassTag](
    parent: DStream[T],
    val output: ConcurrentLinkedQueue[Seq[Seq[T]]] = new ConcurrentLinkedQueue[Seq[Seq[T]]](),
    rddOperation: Option[(RDD[T], Time) => Array[Seq[T]]])
    extends ForEachDStream[T](
      parent, { (rdd: RDD[T], t: Time) =>
        val rddOpToApply =
          rddOperation.getOrElse((rdd: RDD[T], _: Time) => rdd.glom().collect().map(_.toSeq))
        val resultsInABatch = rddOpToApply(rdd, t)
        output.add(resultsInABatch)
      },
      false
    ) {

  // This is to clear the output buffer every it is read from a checkpoint
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }
}

private[eventhubs] trait EventHubTestSuiteBase extends TestSuiteBase {

  self: SharedUtils =>

  protected def checkpointDirectory: String = {
    val dir = Utils.createTempDir()
    logDebug(s"checkpointDir: $dir")
    dir.toString
  }

  def setupMultiEventHubStreams[V: ClassTag](
      simulatedEventHubs1: SimulatedEventHubs,
      simulatedEventHubs2: SimulatedEventHubs,
      eventhubsParams1: Map[String, Map[String, String]],
      eventhubsParams2: Map[String, Map[String, String]],
      namespace1: String,
      namespace2: String,
      operation: (EventHubDirectDStream, EventHubDirectDStream) => DStream[V]): StreamingContext = {

    // Setup the stream computation
    val inputStream1 = setupEventHubInputStream(namespace1, simulatedEventHubs1, eventhubsParams1)
    val inputStream2 = setupEventHubInputStream(namespace2, simulatedEventHubs2, eventhubsParams2)
    val operatedStream = operation(inputStream1, inputStream2)
    val outputStream =
      new TestEventHubOutputStream(operatedStream, new ConcurrentLinkedQueue[Seq[Seq[V]]], None)
    outputStream.register()
    ssc
  }

  def setupSingleEventHubStream[V: ClassTag](
      simulatedEventHubs: SimulatedEventHubs,
      eventhubsParams: Map[String, Map[String, String]],
      operation: EventHubDirectDStream => DStream[V],
      rddOperation: Option[(RDD[V], Time) => Array[Seq[V]]]): StreamingContext = {

    // Setup the stream computation
    val inputStream =
      setupEventHubInputStream(eventhubNamespace, simulatedEventHubs, eventhubsParams)
    val operatedStream = operation(inputStream)
    val outputStream = new TestEventHubOutputStream(operatedStream,
                                                    new ConcurrentLinkedQueue[Seq[Seq[V]]],
                                                    rddOperation)
    outputStream.register()
    ssc
  }

  def setupEventHubInputStream(
      namespace: String,
      simulatedEventHubs: SimulatedEventHubs,
      eventhubsParams: Map[String, Map[String, String]]): EventHubDirectDStream = {

    val maxOffsetForEachEventHub =
      EventHubsTestUtilities.getHighestOffsetPerPartition(simulatedEventHubs)

    new EventHubDirectDStream(
      ssc,
      progressRootPath.toString,
      eventhubsParams,
      (ehParams: Map[String, String]) =>
        new TestEventHubsClient(ehParams, simulatedEventHubs, maxOffsetForEachEventHub)
    )
  }

  def runEventHubStreams[V: ClassTag](ssc: StreamingContext,
                                      numBatches: Int,
                                      numExpectedOutput: Int): Seq[Seq[V]] = {
    // Flatten each RDD into a single Seq
    runEventHubStreamsWithPartitions(ssc, numBatches, numExpectedOutput).map(_.flatten.toSeq)
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   *
   * Returns a sequence of RDD's. Each RDD is represented as several sequences of items, each
   * representing one partition.
   *
   * This function is copied from Spark code base and modified by changing the TestOutputStream
   * implementation
   */
  def runEventHubStreamsWithPartitions[V: ClassTag](ssc: StreamingContext,
                                                    numBatches: Int,
                                                    numExpectedOutput: Int): Seq[Seq[Seq[V]]] = {

    import scala.collection.JavaConverters._

    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(numExpectedOutput > 0, "Number of expected outputs after " + numBatches + " is zero")
    logInfo("numBatches = " + numBatches + ", numExpectedOutput = " + numExpectedOutput)

    // Get the output buffer
    val outputStream = ssc.graph.getOutputStreams
      .filter(_.isInstanceOf[TestEventHubOutputStream[_]])
      .head
      .asInstanceOf[TestEventHubOutputStream[V]]
    val output = outputStream.output

    try {
      // Start computation
      ssc.start()

      // Advance manual clock
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      logInfo("Manual clock before advancing = " + clock.getTimeMillis())
      if (actuallyWait) {
        for (_ <- 1 to numBatches) {
          logInfo("Actually waiting for " + batchDuration)
          clock.advance(batchDuration.milliseconds)
          Thread.sleep(batchDuration.milliseconds)
        }
      } else {
        clock.advance(numBatches * batchDuration.milliseconds)
      }
      logInfo("Manual clock after advancing = " + clock.getTimeMillis())

      // Wait until expected number of output items have been generated
      val startTime = System.currentTimeMillis()
      while (output.size < numExpectedOutput &&
             System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
        logInfo("output.size = " + output.size + ", numExpectedOutput = " + numExpectedOutput)
        ssc.awaitTerminationOrTimeout(50)
      }
      val timeTaken = System.currentTimeMillis() - startTime
      logInfo("Output generated in " + timeTaken + " milliseconds")
      output.asScala.foreach(x => logInfo("[" + x.mkString(",") + "]"))
      assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
      assert(output.size === numExpectedOutput, "Unexpected number of outputs generated")

      Thread.sleep(100) // Give some time for the forgetting old RDDs to complete
    } finally {
      ssc.stop(stopSparkContext = true)
    }
    output.asScala.toSeq
  }

  protected def createSimulatedEventHub[U: ClassTag](
      namespace: String,
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]]): SimulatedEventHubs = {
    val ehAndRawInputMap = eventhubsParams.keys.flatMap { eventHubName =>
      val ehList = {
        for (i <- 0 until eventhubsParams(eventHubName)("eventhubs.partition.count").toInt)
          yield NameAndPartition(eventHubName, i)
      }.toArray
      ehList.zip(input)
    }.toMap
    new SimulatedEventHubs(
      namespace,
      ehAndRawInputMap.map {
        case (eventHubNameAndPartition, propertyQueue) =>
          (eventHubNameAndPartition,
           EventHubsTestUtilities.generateEventData(
             propertyQueue.map(property => ('e', Seq(property))),
             eventHubNameAndPartition.partitionId,
             0))
      }
    )
  }

  protected def verifyOffsetsAndSeqs(ssc: StreamingContext,
                                     namespace: String,
                                     expectedOffsetsAndSeqs: Map[String, OffsetRecord]): Unit = {
    val producedOffsetsAndSeqs = ssc.graph
      .getInputStreams()
      .filter(_.isInstanceOf[EventHubDirectDStream])
      .map(_.asInstanceOf[EventHubDirectDStream])
      .filter(_.ehNamespace == namespace)
      .map(eventHubStream => (eventHubStream.ehNamespace, eventHubStream.currentOffsetsAndSeqNums))
      .toMap
    assert(expectedOffsetsAndSeqs === producedOffsetsAndSeqs)
  }

  def testProgressTracker(namespace: String,
                          expectedOffsetsAndSeqs: OffsetRecord,
                          timestamp: Long): Unit = {
    val producedOffsetsAndSeqs = DirectDStreamProgressTracker.getInstance
      .asInstanceOf[DirectDStreamProgressTracker]
      .read(namespace, timestamp - batchDuration.milliseconds, fallBack = true)
    assert(producedOffsetsAndSeqs === expectedOffsetsAndSeqs)
  }

  def testBinaryOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      eventhubsParams1: Map[String, Map[String, String]],
      eventhubsParams2: Map[String, Map[String, String]],
      expectedOffsetsAndSeqs1: Map[String, OffsetRecord],
      expectedOffsetsAndSeqs2: Map[String, OffsetRecord],
      operation: (EventHubDirectDStream, EventHubDirectDStream) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      numBatches: Int = -1) {

    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    // transform input to EventData instances
    val simulatedEventHubs1 = createSimulatedEventHub("namespace1", input1, eventhubsParams1)
    val simulatedEventHubs2 = createSimulatedEventHub("namespace2", input2, eventhubsParams2)

    withStreamingContext(
      setupMultiEventHubStreams(simulatedEventHubs1,
                                simulatedEventHubs2,
                                eventhubsParams1,
                                eventhubsParams2,
                                "namespace1",
                                "namespace2",
                                operation)) { ssc =>
      runStreamsWithEventHubInput(ssc, numBatches_, expectedOutput, useSet = true)
    }
    verifyOffsetsAndSeqs(ssc, "namespace1", expectedOffsetsAndSeqs1)
    verifyOffsetsAndSeqs(ssc, "namespace2", expectedOffsetsAndSeqs2)
  }

  protected def runStreamsWithEventHubInput[V: ClassTag](ssc: StreamingContext,
                                                         numBatches: Int,
                                                         expectedOutput: Seq[Seq[V]],
                                                         useSet: Boolean): Unit = {
    val output = runEventHubStreams[V](ssc, numBatches, expectedOutput.size)
    verifyOutput[V](output, expectedOutput, useSet)
  }

  private def setupFluctuatedInputStream(
      namespace: String,
      simulatedEventHubs: SimulatedEventHubs,
      messagesBeforeEmpty: Long,
      numBatchesBeforeNewData: Int,
      eventhubsParams: Map[String, Map[String, String]]): EventHubDirectDStream = {

    val maxOffsetForEachEventHub = simulatedEventHubs.messageStore.map {
      case (ehNameAndPartition, messageQueue) =>
        (ehNameAndPartition, (messageQueue.length.toLong - 1, messageQueue.length.toLong - 1))
    }
    new EventHubDirectDStream(
      ssc,
      progressRootPath.toString,
      eventhubsParams,
      (ehParams: Map[String, String]) =>
        new FluctuatedEventHubClient(ehParams,
                                     simulatedEventHubs,
                                     ssc,
                                     messagesBeforeEmpty,
                                     numBatchesBeforeNewData,
                                     maxOffsetForEachEventHub)
    )
  }

  private def setupFluctuatedEventHubStream[V: ClassTag](
      simulatedEventHubs: SimulatedEventHubs,
      eventhubsParams: Map[String, Map[String, String]],
      operation: EventHubDirectDStream => DStream[V],
      messagesBeforeEmpty: Long,
      numBatchesBeforeNewData: Int): StreamingContext = {

    val inputStream = setupFluctuatedInputStream(eventhubNamespace,
                                                 simulatedEventHubs,
                                                 messagesBeforeEmpty,
                                                 numBatchesBeforeNewData,
                                                 eventhubsParams)
    val operatedStream = operation(inputStream)
    val outputStream =
      new TestEventHubOutputStream(operatedStream, new ConcurrentLinkedQueue[Seq[Seq[V]]], None)
    outputStream.register()
    ssc
  }

  def testFluctuatedStream[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]],
      expectedOffsetsAndSeqs: Map[String, OffsetRecord],
      operation: EventHubDirectDStream => DStream[V],
      expectedOutput: Seq[Seq[V]],
      messagesBeforeEmpty: Long,
      numBatchesBeforeNewData: Int) {

    val numBatches_ = expectedOutput.size
    val simulatedEventHubs = createSimulatedEventHub(eventhubNamespace, input, eventhubsParams)

    withStreamingContext(
      setupFluctuatedEventHubStream(simulatedEventHubs,
                                    eventhubsParams,
                                    operation,
                                    messagesBeforeEmpty,
                                    numBatchesBeforeNewData)) { ssc =>
      runStreamsWithEventHubInput(ssc, numBatches_, expectedOutput, useSet = false)
    }
    verifyOffsetsAndSeqs(ssc, eventhubNamespace, expectedOffsetsAndSeqs)
  }

  def testUnaryOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      eventhubsParams: Map[String, Map[String, String]],
      expectedOffsetsAndSeqs: Map[String, OffsetRecord],
      operation: EventHubDirectDStream => DStream[V],
      expectedOutput: Seq[Seq[V]],
      numBatches: Int = -1,
      useSet: Boolean = false,
      rddOperation: Option[(RDD[V], Time) => Array[Seq[V]]] = None) {

    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    // transform input to EventData instances
    val simulatedEventHubs = createSimulatedEventHub(eventhubNamespace, input, eventhubsParams)

    withStreamingContext(
      setupSingleEventHubStream(simulatedEventHubs, eventhubsParams, operation, rddOperation)) {
      ssc =>
        runStreamsWithEventHubInput(ssc, numBatches_, expectedOutput, useSet)
    }
    verifyOffsetsAndSeqs(ssc, eventhubNamespace, expectedOffsetsAndSeqs)
  }
}
