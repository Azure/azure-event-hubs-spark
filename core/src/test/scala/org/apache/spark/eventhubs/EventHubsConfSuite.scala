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

import org.apache.spark.eventhubs.utils.{AadAuthenticationCallbackMock, EventHubsTestUtils, MetricPluginMock, ThrottlingStatusPluginMock}
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

    assert(map(ConnectionStringKey) == EventHubsUtils.encrypt(expectedConnStr))
    assert(map(ConsumerGroupKey) == "consumerGroup")
    intercept[Exception] { map(StartingPositionKey) }
    assert(map(StartingPositionsKey) == expectedPositions)
    assert(map(MaxRatePerPartitionKey).toRate == DefaultMaxRate)
    intercept[Exception] { map(MaxRatesPerPartitionKey) }
    intercept[Exception] { map(ReceiverTimeoutKey) }
    intercept[Exception] { map(MaxSilentTimeKey) }
    intercept[Exception] { map(OperationTimeoutKey) }
    intercept[Exception] { map(MaxEventsPerTriggerKey) }
    assert(map(UseSimulatedClientKey).toBoolean)
    intercept[Exception] { map(UseAadAuthKey) }
    intercept[Exception] { map(AadAuthCallbackKey) }
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
        ConnectionStringKey -> EventHubsUtils.encrypt(expectedConnStr),
        ConsumerGroupKey -> "consumerGroup",
        StartingPositionKey -> Serialization.write(expectedPosition),
        StartingPositionsKey -> Serialization.write(expectedPositions.map {
          case (k, v) => k.toString -> v
        }),
        MaxEventsPerTriggerKey -> 4.toString,
        UseAadAuthKey -> "true",
        AadAuthCallbackKey -> classOf[AadAuthenticationCallbackMock].getName
      ))

    val expectedConf = EventHubsConf(expectedConnStr)
      .setConsumerGroup("consumerGroup")
      .setStartingPosition(expectedPosition)
      .setStartingPositions(expectedPositions)
      .setMaxEventsPerTrigger(4L)
      .setAadAuthCallback(new AadAuthenticationCallbackMock())

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

  test("metricPlugin set/get") {

    val expectedListener = new MetricPluginMock

    val conf = testUtils.getEventHubsConf().setMetricPlugin(expectedListener)
    val actualListener = conf.metricPlugin().get
    val idField = actualListener.getClass.getDeclaredField("id")
    idField.setAccessible(true)
    assert(idField.getInt(actualListener) == expectedListener.id)
  }

  test("throttlingStatusPlugin set/get") {
    val expectedListener = new ThrottlingStatusPluginMock
    val conf = testUtils.getEventHubsConf().setThrottlingStatusPlugin(expectedListener)
    val actualListener = conf.throttlingStatusPlugin.get
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
      .setMaxSilentTime(Duration.ofSeconds(60))
      .setOperationTimeout(Duration.ofSeconds(10))
      .setThreadPoolSize(16)
      .setPrefetchCount(100)
      .setAadAuthCallback(new AadAuthenticationCallbackMock())
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
    originalConf("eventhubs.maxSilentTime")
    originalConf("eventhubs.operationTimeout")
    originalConf("eventhubs.prefetchCount")
    originalConf("eventhubs.threadPoolSize")
    originalConf("eventhubs.useExclusiveReceiver")
    originalConf("maxEventsPerTrigger")
    originalConf("useSimulatedClient")
    originalConf("eventhubs.useAadAuth")
    originalConf("eventhubs.aadAuthCallback")

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
    newConf("eventhubs.maxSilentTime")
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

  test("validate partitions strategy config") {
    val eventHubConfig = testUtils.getEventHubsConf()

    //check default options to make sure it defaults to hash
    assert(
      eventHubConfig.partitionPreferredLocationStrategy ==
        PartitionPreferredLocationStrategy.Hash)

    //check exceptions
    intercept[IllegalStateException] {
      eventHubConfig
        .set(EventHubsConf.PartitionPreferredLocationStrategyKey, "illegalconfigoption")
        .partitionPreferredLocationStrategy
    }

    //check setting string types match expected types
    eventHubConfig.set(EventHubsConf.PartitionPreferredLocationStrategyKey, "Hash")
    assert(
      eventHubConfig.partitionPreferredLocationStrategy ==
        PartitionPreferredLocationStrategy.Hash)
    eventHubConfig.set(EventHubsConf.PartitionPreferredLocationStrategyKey, "BalancedHash")
    assert(
      eventHubConfig.partitionPreferredLocationStrategy ==
        PartitionPreferredLocationStrategy.BalancedHash)
  }

  test("validate - receiver and operation timeout") {
    val eventHubConfig = testUtils.getEventHubsConf()
    intercept[IllegalArgumentException] {
      eventHubConfig.setOperationTimeout(Duration.ofMinutes(10))
      eventHubConfig.setReceiverTimeout(Duration.ofMinutes(11))
    }
    intercept[IllegalArgumentException] {
      eventHubConfig.setReceiverTimeout(Duration.ofMinutes(2))
      eventHubConfig.setOperationTimeout(Duration.ofMinutes(1))
    }

    eventHubConfig.setReceiverTimeout(Duration.ofMinutes(2))
    assert(eventHubConfig.receiverTimeout.get.toMinutes == 2)

    eventHubConfig.setOperationTimeout(Duration.ofMinutes(3))
    assert(eventHubConfig.operationTimeout.get.toMinutes == 3)
  }

  test("validate - max silent time") {
    val eventHubConfig = testUtils.getEventHubsConf()
    intercept[IllegalArgumentException] {
      eventHubConfig.setMaxSilentTime(Duration.ofSeconds(29))
    }

    eventHubConfig.setMaxSilentTime(Duration.ofMinutes(1))
    assert(eventHubConfig.maxSilentTime.get.toMinutes == 1)
  }

  test("validate - slow partition adjustment config") {
    val eventHubConfig = testUtils.getEventHubsConf()

    // check the default value. It should be DefaultSlowPartitionAdjustment = false
    assert(
      eventHubConfig.slowPartitionAdjustment ==
        DefaultSlowPartitionAdjustment.toBoolean)

    val expectedSlowPartionAdjustment = true
    eventHubConfig.setSlowPartitionAdjustment(expectedSlowPartionAdjustment)
    val actualSlowPartionAdjustment = eventHubConfig.slowPartitionAdjustment
    assert(expectedSlowPartionAdjustment == actualSlowPartionAdjustment)
  }

  test("validate - max acceptable batch receive time config") {
    val eventHubConfig = testUtils.getEventHubsConf()
    val expectedTime = Duration.ofSeconds(20)
    eventHubConfig.setMaxAcceptableBatchReceiveTime(expectedTime)
    val actualTime = eventHubConfig.maxAcceptableBatchReceiveTime.get
    assert(expectedTime == actualTime)
  }


  test("validate - AadAuthenticationCallback") {
    val aadAuthCallback = new AadAuthenticationCallbackMock()
    val eventHubConfig = testUtils.getEventHubsConf()
      .setAadAuthCallback(aadAuthCallback)

    val actualCallback = eventHubConfig.aadAuthCallback()
    assert(eventHubConfig.useAadAuth)
    assert(actualCallback.get.isInstanceOf[AadAuthenticationCallbackMock])
  }
}
