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

package org.apache.spark.sql.streaming.eventhubs

import java.io.{ BufferedWriter, FileInputStream, OutputStream, OutputStreamWriter }
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition }
import org.apache.spark.eventhubs.utils.{ EventHubsTestUtils, SimulatedClient }
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{ count, window }
import org.apache.spark.sql.streaming.{ ProcessingTime, StreamTest }
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

abstract class EventHubsSourceTest extends StreamTest with SharedSQLContext {

  protected var testUtils: EventHubsTestUtils = _

  override def beforeAll: Unit = {
    super.beforeAll
    testUtils = new EventHubsTestUtils
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.destroyAllEventHubs()
      testUtils = null
    }
    super.afterAll()
  }

  override val streamingTimeout = 30.seconds

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because EventHubsSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q.processAllAvailable()
    true
  }

  case class AddEventHubsData(conf: EventHubsConf, data: Int*)(implicit concurrent: Boolean = false,
                                                               message: String = "")
      extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      if (query.get.isActive) {
        // Make sure no Spark job is running when deleting a topic
        query.get.processAllAvailable()
      }

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[EventHubsSource] =>
          source.asInstanceOf[EventHubsSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find EventHubs source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the EventHubs source in the StreamExecution logical plan as there" +
            "are multiple EventHubs sources:\n\t" + sources.mkString("\n\t"))
      }

      val ehSource = sources.head
      testUtils.send(conf.name, data)

      val seqNos = testUtils.getLatestSeqNos(conf)
      require(seqNos.size == testUtils.getEventHubs(conf.name).partitionCount)

      val offset = EventHubsSourceOffset(seqNos)
      logInfo(s"Added data, expected offset $offset")
      (ehSource, offset)
    }

    override def toString: String = {
      s"AddEventHubsData(data: $data)"
    }
  }
}

class EventHubsSourceSuite extends EventHubsSourceTest {

  import testImplicits._
  import EventHubsTestUtils._

  private val eventHubsId = new AtomicInteger(0)

  def newEventHubs(): String = {
    s"eh-${eventHubsId.getAndIncrement()}"
  }

  private def getEventHubsConf(ehName: String): EventHubsConf = testUtils.getEventHubsConf(ehName)

  testWithUninterruptibleThread("deserialization of initial offset with Spark 2.1.0") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    withTempDir { metadataPath =>
      val parameters =
        getEventHubsConf(eventHub.name).toMap

      val source = new EventHubsSource(sqlContext,
                                       parameters,
                                       SimulatedClient.apply,
                                       metadataPath.getAbsolutePath,
                                       true)

      source.getOffset.get // Write initial offset

      // Make sure Spark 2.1.0 will throw an exception when reading the new log
      intercept[java.lang.IllegalArgumentException] {
        // Simulate how Spark 2.1.0 reads the log
        Utils.tryWithResource(new FileInputStream(metadataPath.getAbsolutePath + "/0")) { in =>
          val length = in.read()
          val bytes = new Array[Byte](length)
          in.read(bytes)
          EventHubsSourceOffset(SerializedOffset(new String(bytes, UTF_8)))
        }
      }
    }
  }

  testWithUninterruptibleThread("deserialization of initial offset written by future version") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    withTempDir { metadataPath =>
      val futureMetadataLog =
        new HDFSMetadataLog[EventHubsSourceOffset](sqlContext.sparkSession,
                                                   metadataPath.getAbsolutePath) {
          override def serialize(metadata: EventHubsSourceOffset, out: OutputStream): Unit = {
            out.write(0)
            val writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8))
            writer.write(s"v99999\n${metadata.json}")
            writer.flush()
          }
        }

      val eh = newEventHubs()
      val parameters = getEventHubsConf(eh).toMap

      val offset = EventHubsSourceOffset((eh, 0, 0L), (eh, 1, 0L), (eh, 2, 0L))
      futureMetadataLog.add(0, offset)

      val source = new EventHubsSource(sqlContext,
                                       parameters,
                                       SimulatedClient.apply,
                                       metadataPath.getAbsolutePath,
                                       true)

      val e = intercept[java.lang.IllegalStateException] {
        source.getOffset.get // Read initial offset
      }

      Seq(
        s"maximum supported log version is v${EventHubsSource.VERSION}, but encountered v99999",
        "produced by a newer version of Spark and cannot be read by this version"
      ).foreach { message =>
        assert(e.getMessage.contains(message))
      }
    }
  }

  test("(de)serialization of initial offsets") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    val parameters = getEventHubsConf(eventHub.name).toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    testStream(reader.load())(makeSureGetOffsetCalled, StopStream, StartStream(), StopStream)
  }

  test("maxSeqNosPerTrigger") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    val parameters =
      getEventHubsConf(eventHub.name)
        .setMaxEventsPerTrigger(4)
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // we'll get one event per partition per trigger
      CheckAnswer(0, 0, 0, 0),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 0, 0, 0, 1, 1, 1, 1),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3)
    )
  }

  test("maxOffsetsPerTrigger with non-uniform partitions") {
    val name = newEventHubs()
    val eventHub = testUtils.createEventHubs(name, DefaultPartitionCount)

    testUtils.send(name, 0, 100 to 200)
    testUtils.send(name, 1, 10 to 20)
    testUtils.send(name, 2, Seq(1))
    // partition 3 of 3 remains empty.

    val parameters =
      getEventHubsConf(name)
        .setMaxEventsPerTrigger(10)
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(e => e.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // 1 from smallest, 1 from middle, 8 from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107, 11, 108, 109, 110, 111, 112, 113,
        114, 115, 116),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107, 11, 108, 109, 110, 111, 112, 113,
        114, 115, 116, 12, 117, 118, 119, 120, 121, 122, 123, 124, 125),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107, 11, 108, 109, 110, 111, 112, 113,
        114, 115, 116, 12, 117, 118, 119, 120, 121, 122, 123, 124, 125, 13, 126, 127, 128, 129, 130,
        131, 132, 133, 134)
    )
  }

  test("cannot stop EventHubs stream") {
    val eh = newEventHubs()
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)
    testUtils.populateUniformly(eh, 5000)

    val parameters = getEventHubsConf(eh).toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  for (failOnDataLoss <- Seq(true, false)) {
    test(s"assign from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val eh = newEventHubs()
      testFromLatestSeqNos(eh, failOnDataLoss = failOnDataLoss)
    }

    test(s"assign from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val eh = newEventHubs()
      testFromEarliestSeqNos(eh, failOnDataLoss = failOnDataLoss)
    }

    test(s"assign from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val eh = newEventHubs()
      testFromSpecificSeqNos(eh, failOnDataLoss = failOnDataLoss)
    }
  }

  private def testFromLatestSeqNos(eh: String, failOnDataLoss: Boolean): Unit = {
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)
    testUtils.send(eh, 0, Seq(-1))

    require(testUtils.getEventHubs(eh).getPartitions.size === 4)

    // In practice, we would use Position.fromEndOfStream which would
    // translate to the configuration below.
    val positions = Map(
      0 -> EventPosition.fromSequenceNumber(1, isInclusive = true),
      1 -> EventPosition.fromSequenceNumber(0, isInclusive = true),
      2 -> EventPosition.fromSequenceNumber(0, isInclusive = true),
      3 -> EventPosition.fromSequenceNumber(0, isInclusive = true)
    )

    val conf = getEventHubsConf(eh)
      .setStartingPositions(positions)
      .setFailOnDataLoss(failOnDataLoss)

    val reader = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)

    val eventhubs = reader
      .load()
      .selectExpr("body")
      .as[String]

    val mapped: Dataset[Int] = eventhubs.map(_.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddEventHubsData(conf, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddEventHubsData(conf, 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddEventHubsData(conf, 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AddEventHubsData(conf, 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestSeqNos(eh: String, failOnDataLoss: Boolean): Unit = {
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)

    require(testUtils.getEventHubs(eh).getPartitions.size === 4)
    testUtils.send(eh, 1 to 3) // round robin events across partitions

    val conf = getEventHubsConf(eh)
      .setFailOnDataLoss(failOnDataLoss)

    val reader = spark.readStream
    reader
      .format(classOf[EventHubsSourceProvider].getCanonicalName.stripSuffix("$"))
      .options(conf.toMap)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped = eventhubs.map(e => e.toInt + 1)

    testStream(mapped)(
      AddEventHubsData(conf, 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddEventHubsData(conf, 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AddEventHubsData(conf, 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromSpecificSeqNos(eh: String, failOnDataLoss: Boolean): Unit = {
    testUtils.createEventHubs(eh, partitionCount = 5)

    require(testUtils.getEventHubs(eh).getPartitions.size === 5)

    val positions = Map(
      0 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
      1 -> EventPosition.fromSequenceNumber(3L, isInclusive = true),
      2 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
      3 -> EventPosition.fromSequenceNumber(1L, isInclusive = true),
      4 -> EventPosition.fromSequenceNumber(2L, isInclusive = true)
    )

    val conf = getEventHubsConf(eh)
      .setStartingPositions(positions)
      .setFailOnDataLoss(failOnDataLoss)

    // partition 0 starts at the earliest sequence numbers, these should all be seen
    testUtils.send(eh, 0, Seq(-20, -21, -22))
    // partition 1 starts at the latest sequence numbers, these should all be skipped
    testUtils.send(eh, 1, Seq(-10, -11, -12))
    // partition 2 starts at 0, these should all be seen
    testUtils.send(eh, 2, Seq(0, 1, 20))
    // partition 3 starts at 1, first should be skipped
    testUtils.send(eh, 3, Seq(10, 11, 12))
    // partition 4 starts at 2, first and second should be skipped
    testUtils.send(eh, 4, Seq(20, 21, 22))

    val reader = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(e => e.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      CheckAnswer(-20, -21, -22, 0, 1, 20, 11, 12, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-20, -21, -22, 0, 1, 20, 11, 12, 22), // Should get the data back on recovery
      AddEventHubsData(conf, 30, 31, 32, 33, 34),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22, 30, 31, 32, 33, 34),
      StopStream
    )
  }

  test("input row metrics") {
    val eh = newEventHubs()
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)

    testUtils.send(eh, Seq(-1))
    require(testUtils.getEventHubs(eh).getPartitions.size === 4)

    val positions = Map(
      0 -> EventPosition.fromSequenceNumber(1L, isInclusive = true),
      1 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
      2 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
      3 -> EventPosition.fromSequenceNumber(0L, isInclusive = true)
    )

    val conf = getEventHubsConf(eh)
      .setStartingPositions(positions)

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)
      .load()
      .select("body")
      .as[String]

    val mapped = eventhubs.map(e => e.toInt + 1)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddEventHubsData(conf, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  test("EventHubs column types") {
    val now = System.currentTimeMillis()
    val eh = newEventHubs()
    testUtils.createEventHubs(eh, partitionCount = 1)

    val conf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setStartingPosition(EventPosition.fromSequenceNumber(0L, isInclusive = true))

    require(testUtils.getEventHubs(eh).getPartitions.size === 1)

    testUtils.send(eh, Seq(1))

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)
      .load()

    val query = eventhubs.writeStream
      .format("memory")
      .outputMode("append")
      .queryName("eventhubsColumnTypes")
      .start()

    query.processAllAvailable()
    val rows = spark.table("eventhubsColumnTypes").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    assert(row.getAs[String]("body") === "1", s"Unexpected results: $row")
    assert(row.getAs[Long]("offset") === 0L, s"Unexpected results: $row")
    assert(row.getAs[Long]("seqNumber") === 0, s"Unexpected results: $row")
    assert(row.getAs[String]("publisher") === null, s"Unexpected results: $row")
    assert(row.getAs[String]("partitionKey") === null, s"Unexpected results: $row")
    // We cannot check the exact timestamp as it's the time that messages were inserted by the
    // producer. So here we just use a low bound to make sure the internal conversion works.
    assert(row.getAs[java.sql.Timestamp]("enqueuedTime").getTime >= now,
           s"Unexpected results: $row")
    query.stop()
  }

  test("EventHubsSource with watermark") {
    val now = System.currentTimeMillis()
    val eh = newEventHubs()
    testUtils.createEventHubs(eh, partitionCount = 1)

    val conf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setStartingPosition(EventPosition.fromSequenceNumber(0L, isInclusive = true))

    require(testUtils.getEventHubs(eh).getPartitions.size === 1)

    testUtils.send(eh, Seq(1))

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)
      .load()

    val windowedAggregation = eventhubs
      .withWatermark("enqueuedTime", "10 seconds")
      .groupBy(window($"enqueuedTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start") as 'window, $"count")

    val query = windowedAggregation.writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("eventhubsWatermark")
      .start()
    query.processAllAvailable()

    val rows = spark.table("eventhubsWatermark").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    // We cannot check the exact window start time as it depends on the time that messages were
    // inserted by the producer. So here we just use a low bound to make sure the internal
    // conversion works.
    assert(row.getAs[java.util.Date]("window").getTime >= now - 5 * 1000,
           s"Unexpected results: $row")
    assert(row.getAs[Int]("count") === 1, s"Unexpected results: $row")
    query.stop()
  }
}
