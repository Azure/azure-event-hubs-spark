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

import org.apache.spark.eventhubs.utils.{
  ConnectionStringBuilder,
  EventHubsTestUtils,
  EventPosition
}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.scalatest.{ BeforeAndAfterAll, FunSuite }

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
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
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

    val expectedPositions = Serialization.write(
      Map(
        0 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
        1 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
        2 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
        3 -> EventPosition.fromSequenceNumber(0L, isInclusive = true)
      )
    )

    assert(map(ConnectionStringKey) == expectedConnStr)
    assert(map(ConsumerGroupKey) == "consumerGroup")
    intercept[Exception] { map(StartingPositionKey) }
    assert(map(StartingPositionsKey) == expectedPositions)
    assert(map(MaxRatePerPartitionKey).toRate == DefaultMaxRate)
    intercept[Exception] { map(MaxRatesPerPartitionKey) }
    intercept[Exception] { map(ReceiverTimeoutKey) }
    intercept[Exception] { map(OperationTimeoutKey) }
    intercept[Exception] { map(FailOnDataLossKey) }
    intercept[Exception] { map(MaxSeqNosPerTriggerKey) }
    intercept[Exception] { map(UseSimulatedClientKey) }
  }

  test("toConf") {
    val expectedPosition = EventPosition.fromSequenceNumber(20L, isInclusive = true)

    val expectedPositions = Map(
      0 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
      2 -> EventPosition.fromSequenceNumber(0L, isInclusive = true),
      3 -> EventPosition.fromSequenceNumber(0L, isInclusive = true)
    )

    val actualConf = EventHubsConf.toConf(
      Map(
        ConnectionStringKey -> expectedConnStr,
        ConsumerGroupKey -> "consumerGroup",
        StartingPositionKey -> Serialization.write(expectedPosition),
        StartingPositionsKey -> Serialization.write(expectedPositions),
        MaxSeqNosPerTriggerKey -> 4.toString
      ))

    val expectedConf = EventHubsConf(expectedConnStr)
      .setConsumerGroup("consumerGroup")
      .setStartingPosition(expectedPosition)
      .setStartingPositions(expectedPositions)
      .setMaxSeqNosPerTrigger(4L)

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
    assert(conf != cloned)
  }

  test("name") {
    val conf = testUtils.getEventHubsConf("foo")
    assert(conf.name == "foo")
  }

  test("setName") {
    val conf = testUtils.getEventHubsConf("foo")
    val expected = ConnectionStringBuilder(expectedConnStr)
      .setEventHubName("bar")
      .build

    conf.setName("bar")
    assert(conf.connectionString == expected)
  }
}
