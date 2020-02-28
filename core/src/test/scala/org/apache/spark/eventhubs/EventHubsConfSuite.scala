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

package org.apache.spark.eventhubs

import java.time.Duration
import java.util.NoSuchElementException

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.utils.{EventHubsReceiverListener, EventHubsSenderListener, EventHubsTestUtils}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read => sread}
import org.json4s.jackson.Serialization.{write => swrite}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Tests [[EventHubsConf]] for correctness.
 */
class EventHubsConfSuite extends FunSuite with BeforeAndAfterAll {

  import EventHubsConf._
  import EventHubsTestUtils._

  private implicit val formats = Serialization.formats(NoTypeHints)

  private var testUtils: EventHubsTestUtils = _

  override def beforeAll: Unit = {
    testUtils = new EventHubsTestUtils
    testUtils.createEventHubs("name", partitionCount = 4)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.destroyAllEventHubs()
      testUtils = null
    }
  }

  private def expectedConnStr =
    ConnectionStringBuilder()
      .setNamespaceName("namespace")
      .setEventHubName("name")
      .setSasKeyName("keyName")
      .setSasKey("key")
      .build

  test("set throws NullPointerException for null key and value") {
    val ehConf = testUtils.getEventHubsConf()
    intercept[NullPointerException] { ehConf.set(null, "value") }
    intercept[NullPointerException] { ehConf.set("key", null) }
    intercept[NullPointerException] { ehConf.set(null, null) }
  }

  test("set/apply/get are properly working") {
    val ehConf = testUtils.getEventHubsConf().set("some key", "some value")
    assert(ehConf("some key") == "some value")
  }

  test("toMap") {
    val map = testUtils.getEventHubsConf().toMap
    val eh = "name"

    val expectedPositions = Serialization.write(
      Map(
        NameAndPartition(eh, 0) -> EventPosition.fromSequenceNumber(0L),
        NameAndPartition(eh, 1) -> EventPosition.fromSequenceNumber(0L),
        NameAndPartition(eh, 2) -> EventPosition.fromSequenceNumber(0L),
        NameAndPartition(eh, 3) -> EventPosition.fromSequenceNumber(0L)
      ).map { case (k, v) => k.toString -> v }
    )

    assert(map(ConnectionStringKey) == expectedConnStr)
    assert(map(ConsumerGroupKey) == "consumerGroup")
    intercept[Exception] { map(StartingPositionKey) }
    assert(map(StartingPositionsKey) == expectedPositions)
    assert(map(MaxRatePerPartitionKey).toRate == DefaultMaxRate)
    intercept[Exception] { map(MaxRatesPerPartitionKey) }
    intercept[Exception] { map(ReceiverTimeoutKey) }
    intercept[Exception] { map(OperationTimeoutKey) }
    intercept[Exception] { map(MaxEventsPerTriggerKey) }
    assert(map(UseSimulatedClientKey).toBoolean)
  }

  test("toConf") {
    val expectedPosition = EventPosition.fromSequenceNumber(20L)

    val expectedPositions = Map(
      NameAndPartition("name", 0) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition("name", 2) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition("name", 3) -> EventPosition.fromSequenceNumber(0L)
    )

    val actualConf = EventHubsConf.toConf(
      Map(
        ConnectionStringKey -> expectedConnStr,
        ConsumerGroupKey -> "consumerGroup",
        StartingPositionKey -> Serialization.write(expectedPosition),
        StartingPositionsKey -> Serialization.write(expectedPositions.map {
          case (k, v) => k.toString -> v
        }),
        MaxEventsPerTriggerKey -> 4.toString
      ))

    val expectedConf = EventHubsConf(expectedConnStr)
      .setConsumerGroup("consumerGroup")
      .setStartingPosition(expectedPosition)
      .setStartingPositions(expectedPositions)
      .setMaxEventsPerTrigger(4L)

    assert(expectedConf.equals(actualConf))
  }

  test("toMap, toConf: There and back again") {
    val expectedConf = testUtils.getEventHubsConf()

    val actualConf = EventHubsConf.toConf(expectedConf.clone.toMap)

    assert(expectedConf.equals(actualConf))
  }

  test("clone") {
    val conf = testUtils.getEventHubsConf()
    val cloned = conf.clone
    assert(conf.equals(cloned))
    assert(conf ne cloned)
  }

  test("name") {
    val conf = testUtils.getEventHubsConf()
    assert(conf.name == "name")
  }

  test("setName") {
    val conf = testUtils.getEventHubsConf()
    val expected = ConnectionStringBuilder(expectedConnStr)
      .setEventHubName("bar")
      .build

    conf.setName("bar")
    assert(conf.connectionString == expected)
  }

  test("EventPosition serialization") {
    implicit val formats = Serialization.formats(NoTypeHints)

    val expected = EventPosition.fromSequenceNumber(10L)
    val actual = sread[EventPosition](swrite[EventPosition](expected))
    assert(actual.equals(expected))
  }

  test("EventPosition is serialized correctly in EventHubsConf") {
    implicit val formats = Serialization.formats(NoTypeHints)

    val expected = EventPosition.fromSequenceNumber(10L)
    val conf = testUtils.getEventHubsConf().setStartingPosition(expected)
    val actual = conf.startingPosition.get
    assert(actual.equals(expected))
  }

  test("Map of EventPositions can be serialized") {
    implicit val formats = Serialization.formats(NoTypeHints)

    val expected = Map(
      NameAndPartition("name", 0) -> EventPosition.fromSequenceNumber(3L),
      NameAndPartition("name", 1) -> EventPosition.fromSequenceNumber(2L),
      NameAndPartition("name", 2) -> EventPosition.fromSequenceNumber(1L),
      NameAndPartition("name", 3) -> EventPosition.fromSequenceNumber(0L)
    )

    val stringKeys = expected.map { case (k, v) => k.toString -> v }

    val ser = swrite[Map[String, EventPosition]](stringKeys)
    val deser = sread[Map[String, EventPosition]](ser)

    val actual = deser map { case (k, v) => NameAndPartition.fromString(k) -> v }
    assert(actual.equals(expected))
  }

  test("EventPosition Map is serialized in EventHubsConf") {
    implicit val formats = Serialization.formats(NoTypeHints)

    val expected = Map(
      NameAndPartition("name", 0) -> EventPosition.fromSequenceNumber(3L),
      NameAndPartition("name", 1) -> EventPosition.fromSequenceNumber(2L),
      NameAndPartition("name", 2) -> EventPosition.fromSequenceNumber(1L),
      NameAndPartition("name", 3) -> EventPosition.fromSequenceNumber(0L)
    )

    val conf = testUtils.getEventHubsConf().setStartingPositions(expected)
    val actual = conf.startingPositions.get

    assert(actual.equals(expected))
  }

  test("receiverListener serialization / deserialization") {
    val expectedListener = new EventHubsReceiverListener {

      val id = 1

      override def onBatchReceiveSuccess(nAndP: NameAndPartition, elapsedTime: SequenceNumber, batchSize: Rate, receivedBytes: SequenceNumber): Unit = ???

      override def onBatchReceiveSkip(nAndP: NameAndPartition, requestSeqNo: SequenceNumber, batchSize: Rate): Unit = ???

      override def onReceiveFirstEvent(nAndP: NameAndPartition, firstEvent: EventData): Unit = ???
    }

    val conf = testUtils.getEventHubsConf().setReceiverListener(expectedListener)
    val actualListener = conf.receiverListener().get
    val idField = actualListener.getClass.getDeclaredField("id")
    idField.setAccessible(true)
    assert(idField.getInt(actualListener) == expectedListener.id)
  }

  test("senderListener serialization / deserialization") {
    val expectedListener = new EventHubsSenderListener {

      val id = 1

      override def onBatchSendSuccess(messageCount: Rate, messageSizeInBytes: Rate, sendElapsedTimeInNanos: SequenceNumber, retryTimes: Rate): Unit = ???

      override def onBatchSendFail(exception: Throwable): Unit = ???

      override def onWriterOpen(partitionId: SequenceNumber, version: SequenceNumber): Unit = ???

      override def onWriterClose(totalMessageCount: Rate, totalMessageSizeInBytes: Rate, endToEndElapsedTimeInNanos: SequenceNumber): Unit = ???
    }

    val conf = testUtils.getEventHubsConf().setSenderListener(expectedListener)
    val actualListener = conf.senderListener().get
    val idField = actualListener.getClass.getDeclaredField("id")
    idField.setAccessible(true)
    assert(idField.getInt(actualListener) == expectedListener.id)
  }

  test("trimmedConfig") {
    val originalConf = testUtils
      .getEventHubsConf()
      .setStartingPosition(EventPosition.fromStartOfStream)
      .setStartingPositions(Map(NameAndPartition("foo", 0) -> EventPosition.fromEndOfStream))
      .setEndingPosition(EventPosition.fromStartOfStream)
      .setEndingPositions(Map(NameAndPartition("foo", 0) -> EventPosition.fromEndOfStream))
      .setMaxRatePerPartition(1000)
      .setMaxRatesPerPartition(Map(NameAndPartition("foo", 0) -> 12))
      .setMaxEventsPerTrigger(100)
      .setReceiverTimeout(Duration.ofSeconds(10))
      .setOperationTimeout(Duration.ofSeconds(10))
      .setThreadPoolSize(16)
      .setPrefetchCount(100)
      .setUseExclusiveReceiver(true)

    val newConf = originalConf.trimmed

    // original should be unmodified
    originalConf("eventhubs.connectionString")
    originalConf("eventhubs.consumerGroup")
    originalConf("eventhubs.startingPosition")
    originalConf("eventhubs.startingPositions")
    originalConf("eventhubs.endingPosition")
    originalConf("eventhubs.endingPositions")
    originalConf("eventhubs.maxRatePerPartition")
    originalConf("eventhubs.maxRatesPerPartition")
    originalConf("eventhubs.receiverTimeout")
    originalConf("eventhubs.operationTimeout")
    originalConf("eventhubs.prefetchCount")
    originalConf("eventhubs.threadPoolSize")
    originalConf("eventhubs.useExclusiveReceiver")
    originalConf("maxEventsPerTrigger")
    originalConf("useSimulatedClient")

    // newConf should be trimmed
    newConf("eventhubs.connectionString")
    newConf("eventhubs.consumerGroup")
    intercept[NoSuchElementException] { newConf("eventhubs.startingPosition") }
    intercept[NoSuchElementException] { newConf("eventhubs.startingPositions") }
    intercept[NoSuchElementException] { newConf("eventhubs.endingPosition") }
    intercept[NoSuchElementException] { newConf("eventhubs.endingPositions") }
    intercept[NoSuchElementException] { newConf("eventhubs.maxRatePerPartition") }
    intercept[NoSuchElementException] { newConf("eventhubs.maxRatesPerPartition") }
    newConf("eventhubs.receiverTimeout")
    newConf("eventhubs.operationTimeout")
    newConf("eventhubs.prefetchCount")
    newConf("eventhubs.threadPoolSize")
    newConf("eventhubs.useExclusiveReceiver")
    intercept[NoSuchElementException] { newConf("maxEventsPerTrigger") }
    newConf("useSimulatedClient")
  }

  test("validate - with EntityPath") {
    assert(testUtils.getEventHubsConf().validate)
  }

  test("validate - without EntityPath") {
    val without = "Endpoint=ENDPOINT;SharedAccessKeyName=KEY_NAME;SharedAccessKey=KEY"
    intercept[IllegalStateException] { EventHubsConf(without).validate }
  }

}
