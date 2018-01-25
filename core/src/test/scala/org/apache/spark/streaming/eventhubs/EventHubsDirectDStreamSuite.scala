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

import java.io.File
import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.PartitionId
import org.apache.spark.eventhubs.utils.EventHubsTestUtils._
import org.apache.spark.eventhubs.rdd.{ HasOffsetRanges, OffsetRange }
import org.apache.spark.eventhubs.utils.{ EventHubsTestUtils, EventPosition, SimulatedClient }
import org.apache.spark.{ SparkConf, SparkFunSuite }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{
  StreamingListener,
  StreamingListenerBatchCompleted,
  StreamingListenerBatchStarted,
  StreamingListenerBatchSubmitted
}
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.util.Utils
import org.scalatest.concurrent.Eventually
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class EventHubsDirectDStreamSuite
    extends SparkFunSuite
    with BeforeAndAfter
    with Eventually
    with Logging {

  import EventHubsDirectDStreamSuite._

  private var testUtils: EventHubsTestUtils = _

  val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)

  private var ssc: StreamingContext = _
  private var testDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new EventHubsTestUtils
  }

  before {
    setDefaults()
    testUtils.createEventHubs()
  }

  after {
    testUtils.destroyEventHubs()

    if (ssc != null) {
      ssc.stop(stopSparkContext = true)
    }
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  private def getEventHubsConf: EventHubsConf = {
    val positions: Map[PartitionId, EventPosition] = (for {
      partitionId <- 0 until PartitionCount
    } yield partitionId -> EventPosition.fromSequenceNumber(0L, isInclusive = true)).toMap

    EventHubsConf(ConnectionString)
      .setConsumerGroup("consumerGroup")
      .setStartingPositions(positions)
      .setMaxRatePerPartition(MaxRate)
  }

  // Put 'count' events in every simulated EventHubs partition
  private def populateUniformly(count: Int): Unit = {
    for (i <- 0 until PartitionCount) {
      EventHubsTestUtils.eventHubs.send(i, 0 to count)
    }
  }

  test("basic stream receiving with smallest starting sequence number") {
    populateUniformly(EventsPerPartition)
    val ehConf = getEventHubsConf
    val batchInterval = 1000
    val timeoutAfter = 100000
    val expectedTotal = (timeoutAfter / batchInterval) * MaxRate

    ssc = new StreamingContext(sparkConf, Milliseconds(batchInterval))
    val stream = withClue("Error creating direct stream") {
      new EventHubsDirectDStream(ssc, ehConf, SimulatedClient.apply)
    }
    val allReceived = new ConcurrentLinkedQueue[EventData]()

    // hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()
    val tf = stream.transform { rdd =>
      // Get the offset ranges in the RDD
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    tf.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        logInfo(s"${o.name} ${o.partitionId} ${o.fromSeqNo} ${o.untilSeqNo}")
      }
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
        // For each partition, get size of the range in the partition,
        // and the number of items in the partition
        val off = offsetRanges(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilSeqNo - off.fromSeqNo
        Iterator((partSize, rangeSize))
      }.collect

      // Verify whether number of elements in each partition
      // matches with the corresponding offset range
      collected.foreach {
        case (partSize, rangeSize) =>
          assert(partSize == rangeSize, "offset ranges are wrong")
      }
    }

    stream.foreachRDD { rdd =>
      allReceived.addAll(util.Arrays.asList(rdd.collect(): _*))
    }
    ssc.start()
    eventually(timeout(timeoutAfter.milliseconds), interval(batchInterval.milliseconds)) {
      assert(allReceived.size === expectedTotal,
             "didn't get expected number of messages, messages:\n" +
               allReceived.asScala.mkString("\n"))
    }
    ssc.stop()
  }

  test("basic stream receiving from random sequence number") {
    populateUniformly(EventsPerPartition)
    val startSeqNo = scala.util.Random.nextInt % (EventsPerPartition / 2)
    val ehConf = getEventHubsConf
      .setStartingPositions(Map.empty)
      .setStartingPosition(EventPosition.fromSequenceNumber(startSeqNo, isInclusive = true))
    val batchInterval = 1000
    val timeoutAfter = 100000
    val expectedTotal =
      if (EventsPerPartition - startSeqNo + 1 < (timeoutAfter / batchInterval) * MaxRate) {
        EventsPerPartition - startSeqNo + 1
      } else {
        (timeoutAfter / batchInterval) * MaxRate
      }

    ssc = new StreamingContext(sparkConf, Milliseconds(batchInterval))
    val stream = withClue("Error creating direct stream") {
      new EventHubsDirectDStream(ssc, ehConf, SimulatedClient.apply)
    }
    val allReceived = new ConcurrentLinkedQueue[EventData]()

    // hold a reference to the current offset ranges so it can be used downstream
    var offsetRanges = Array[OffsetRange]()
    val tf = stream.transform { rdd =>
      // Get the offset ranges in the RDD
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    tf.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        logInfo(s"${o.name} ${o.partitionId} ${o.fromSeqNo} ${o.untilSeqNo}")
      }
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
        // For each partition, get size of the range in the partition
        // and the number of items in the partition
        val off = offsetRanges(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilSeqNo - off.fromSeqNo
        Iterator((partSize, rangeSize))
      }.collect

      // Verify whether number of elements in each partition
      // matches with the corresponding offset range
      collected.foreach {
        case (partSize, rangeSize) =>
          assert(partSize == rangeSize, "offset ranges are wrong")
      }
    }

    stream.foreachRDD { rdd =>
      allReceived.addAll(util.Arrays.asList(rdd.collect(): _*))
    }
    ssc.start()
    eventually(timeout(timeoutAfter milliseconds), interval(batchInterval milliseconds)) {
      assert(allReceived.size === expectedTotal,
             "didn't get expected number of messages, messages:\n" +
               allReceived.asScala.mkString("\n"))
    }
    ssc.stop()
  }

  test("receiving from largest starting offset") {
    populateUniformly(EventsPerPartition)

    val positions = (for {
      id <- 0 until PartitionCount
    } yield id -> EventPosition.fromSequenceNumber(EventsPerPartition, isInclusive = true)).toMap

    val ehConf =
      getEventHubsConf.setStartingPositions(positions)
    val batchInterval = 1000
    val timeoutAfter = 10000

    ssc = new StreamingContext(sparkConf, Milliseconds(batchInterval))
    val stream = withClue("Error creating direct stream") {
      new EventHubsDirectDStream(ssc, ehConf, SimulatedClient.apply)
    }

    val collectedData = new ConcurrentLinkedQueue[EventData]()
    stream.foreachRDD { rdd =>
      collectedData.addAll(util.Arrays.asList(rdd.collect(): _*))
    }
    ssc.start()
    eventually(timeout(timeoutAfter.milliseconds), interval(batchInterval.milliseconds)) {
      assert(collectedData.isEmpty)
    }
    assert(collectedData.isEmpty)
    ssc.stop()
  }

  // Test to verify offset ranges can be recovered from the checkpoints
  test("offset recovery") {
    testDir = Utils.createTempDir()

    testUtils.destroyEventHubs()
    testUtils.createEventHubs()
    for (i <- 0 until PartitionCount) {
      EventHubsTestUtils.eventHubs.send(i, 0 to 25)
    }

    val ehConf = getEventHubsConf

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    ssc.remember(Seconds(20))
    val stream = withClue("Error creating direct stream") {
      new EventHubsDirectDStream(ssc, ehConf, SimulatedClient.apply)
    }
    val keyedStream = stream.map { event =>
      "key" -> event.getSystemProperties.getSequenceNumber
    }

    ssc.checkpoint(testDir.getAbsolutePath)

    //val collectedData = new ConcurrentLinkedQueue[(String, Int)]()
    val collectedData = new ConcurrentLinkedQueue[EventData]()
    stream.foreachRDD { (rdd: RDD[EventData]) =>
      collectedData.addAll(util.Arrays.asList(rdd.collect(): _*))
    }

    ssc.start()

    eventually(timeout(20 seconds), interval(50 milliseconds)) {
      assert(collectedData.size() === 100)
    }

    ssc.stop()

    // Verify that offset ranges were generated
    val offsetRangesBeforeStop = getOffsetRanges(stream)
    assert(offsetRangesBeforeStop.nonEmpty, "No offset ranges generated")
    assert(
      offsetRangesBeforeStop.head._2.forall { _.fromSeqNo === 0 },
      "starting sequence number not zero"
    )

    logInfo("====== RESTARTING ======")

    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream = ssc.graph.getInputStreams().head.asInstanceOf[DStream[EventData]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRanges(recoveredStream).map { x =>
      (x._1, x._2.toSet)
    }
    assert(recoveredOffsetRanges.nonEmpty, "No offset ranges recovered")
    val earlierOffsetRanges = offsetRangesBeforeStop.map { x =>
      (x._1, x._2.toSet)
    }
    assert(
      recoveredOffsetRanges.forall { or =>
        earlierOffsetRanges.contains((or._1, or._2))
      },
      "Recovered ranges are not the same as the ones generated\n" +
        earlierOffsetRanges + "\n" + recoveredOffsetRanges
    )

    /*
    // Send 25 events to every partition
    for (i <- 0 until PartitionCount) {
      EventHubsTestUtils.eventHubs.send(i, 0 to 25)
    }

    // Restart context, give more data and verify the total at the end
    // If the total is right that means each record has been received only once.
    ssc.start()

    eventually(timeout(20 seconds), interval(50 milliseconds)) {
      assert(collectedData.size() === 200)
    }

    ssc.stop()
   */
  }

  test("Direct EventHubs stream report input information") {
    val ehConf = getEventHubsConf

    testUtils.destroyEventHubs()
    testUtils.createEventHubs()
    for (i <- 0 until PartitionCount) {
      EventHubsTestUtils.eventHubs.send(i, 0 to 25)
    }

    val totalSent = 25 * PartitionCount

    import EventHubsDirectDStreamSuite._
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val collector = new InputInfoCollector
    ssc.addStreamingListener(collector)

    val stream = withClue("Error creating direct stream") {
      new EventHubsDirectDStream(ssc, ehConf, SimulatedClient.apply)
    }

    val allReceived = new ConcurrentLinkedQueue[String]()

    stream.map(_.getBytes.map(_.toChar).mkString).foreachRDD { rdd =>
      allReceived.addAll(util.Arrays.asList(rdd.collect(): _*))
    }

    ssc.start()

    eventually(timeout(20 seconds), interval(200 milliseconds)) {
      assert(allReceived.size === totalSent,
             "didn't get expected number of messages, messages:\n" +
               allReceived.asScala.mkString("\n"))

      // Calculate all the records collected in the StreamingListener.
      assert(collector.numRecordsSubmitted.get() === totalSent)
      assert(collector.numRecordsStarted.get() === totalSent)
      assert(collector.numRecordsCompleted.get() === totalSent)
    }

    ssc.stop()
  }

  /** Get the generated offset ranges from the EventHubsStream */
  private def getOffsetRanges(stream: DStream[EventData]): Seq[(Time, Array[OffsetRange])] = {
    stream.generatedRDDs
      .mapValues { rdd =>
        rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      }
      .toSeq
      .sortBy { _._1 }
  }
}

object EventHubsDirectDStreamSuite {
  val EventsPerPartition: Int = 5000

  class InputInfoCollector extends StreamingListener {
    val numRecordsSubmitted = new AtomicLong(0L)
    val numRecordsStarted = new AtomicLong(0L)
    val numRecordsCompleted = new AtomicLong(0L)

    override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
      numRecordsSubmitted.addAndGet(batchSubmitted.batchInfo.numRecords)
    }

    override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
      numRecordsStarted.addAndGet(batchStarted.batchInfo.numRecords)
    }

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      numRecordsCompleted.addAndGet(batchCompleted.batchInfo.numRecords)
    }
  }
}
