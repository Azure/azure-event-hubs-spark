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

package org.apache.spark.eventhubs.common

import org.scalatest.FunSuite

/**
 * Tests [[EventHubsConf]] for correctness.
 */
class EventHubsConfSuite extends FunSuite {

  // Return an EventHubsConf with dummy data
  private def confWithTestValues: EventHubsConf = {
    EventHubsConf()
      .setNamespace("namespace")
      .setName("name")
      .setKeyName("keyName")
      .setKey("key")
      .setPartitionCount("4")
      .setProgressDirectory("dir")
      .setConsumerGroup("consumerGroup")
  }

  // Tests for get, set, and isValid
  test("set throws NullPointerException for null key and value") {
    val ehConf = EventHubsConf()
    intercept[NullPointerException] { ehConf.set(null, "value") }
    intercept[NullPointerException] { ehConf.set("key", null) }
    intercept[NullPointerException] { ehConf.set(null, null) }
  }

  test("set/apply/get are properly working") {
    val ehConf = EventHubsConf().set("some key", "some value")
    assert(ehConf("some key") == "some value")
  }

  test("isValid doesn't return true until all required data is provided") {
    val ehConf = EventHubsConf()
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setNamespace("namespace")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setName("name")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setKeyName("keyName")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setKey("key")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setPartitionCount("2")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setProgressDirectory("dir")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setConsumerGroup("consumerGroup")
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setStartOfStream(true) // some starting point must be set before true is returned.
    assert(ehConf.isValid)
  }

  test(
    "isValid doesn't return true when startOfStream is set with endOfStream, startOffsets, or startEnqueueTimes") {
    val ehConf = confWithTestValues
    ehConf.setStartOfStream(true)
    assert(ehConf.isValid)

    // When both are true, and exception is thrown.
    ehConf.setEndOfStream(true)
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.setEndOfStream(false)
    ehConf.setStartOffsets(50)
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.clearStartOffsets()
    ehConf.setStartEnqueueTimes(50)
    intercept[IllegalArgumentException] { ehConf.isValid }
  }

  test(
    "isValid doesn't return true when endOfStream is set along with startOffsets and startEnqueueTimes.") {
    val ehConf = confWithTestValues
    ehConf.setEndOfStream(true)
    assert(ehConf.isValid)

    ehConf.setStartOffsets(50)
    intercept[IllegalArgumentException] { ehConf.isValid }

    ehConf.clearStartOffsets()
    ehConf.setStartEnqueueTimes(50)
    intercept[IllegalArgumentException] { ehConf.isValid }
  }

  test("isValid doesn't return true when startOffsets and startEnqueueTimes are set.") {
    val ehConf = confWithTestValues
    ehConf.setStartOffsets(50)
    assert(ehConf.isValid)

    ehConf.setStartEnqueueTimes(50)
    intercept[IllegalArgumentException] { ehConf.isValid }
  }

  // Tests for toString methods
  test("offsetsToString returns the correct string") {
    val expected = "0:50,1:50,2:50,3:50"
    val ehConf = confWithTestValues
    ehConf.setStartOffsets(0 to 3, 50)
    val actual = EventHubsConf.offsetsToString(ehConf.startOffsets)
    assert(actual == expected)
  }

  test("offsetsToString returns the correct string when partitions are missing") {
    val expected = "0:50,1:50,3:100"
    val ehConf = confWithTestValues
    ehConf.setStartOffsets(0 to 1, 50)
    ehConf.setStartOffsets(3 to 3, 100)
    val actual = EventHubsConf.offsetsToString(ehConf.startOffsets)
    assert(actual == expected)
  }

  test("enqueueTimesToString") {
    val expected = "0:50,1:50,2:50,3:50"
    val ehConf = confWithTestValues
    ehConf.setStartEnqueueTimes(0 to 3, 50)
    val actual = EventHubsConf.enqueueTimesToString(ehConf.startEnqueueTimes)
    assert(actual == expected)
  }

  test("enqueueTimesToString with missing partitions") {
    val expected = "0:50,1:50,3:100"
    val ehConf = confWithTestValues
    ehConf.setStartEnqueueTimes(0 to 1, 50)
    ehConf.setStartEnqueueTimes(3 to 3, 100)
    val actual = EventHubsConf.enqueueTimesToString(ehConf.startEnqueueTimes)
    assert(actual == expected)
  }

  test("maxRatesToString") {
    val expected = "0:50,1:50,2:50,3:50"
    val ehConf = confWithTestValues
    ehConf.setMaxRatePerPartition(0 to 3, 50)
    val actual = EventHubsConf.maxRatesToString(ehConf.maxRatesPerPartition)
    assert(actual == expected)
  }

  test("maxRatesToString with missing partitions") {
    val expected = "0:25,1:50,3:100"
    val ehConf = confWithTestValues
    ehConf.setMaxRatePerPartition(0 to 0, 25)
    ehConf.setMaxRatePerPartition(1 to 1, 50)
    ehConf.setMaxRatePerPartition(3 to 3, 100)
    val actual = EventHubsConf.maxRatesToString(ehConf.maxRatesPerPartition)
    assert(actual == expected)
  }

  // Tests for parse methods
  test("parseOffsets") {
    val ehConf = confWithTestValues
    val expected = ehConf.setStartOffsets(0 to 3, 50).startOffsets

    val offsets = "0:50,1:50,2:50,3:50"
    val actual = EventHubsConf.parseOffsets(offsets)
    assert(actual == expected)
  }

  test("parseEnqueueTimes") {
    val ehConf = confWithTestValues
    val expected = ehConf.setStartEnqueueTimes(0 to 3, 50).startEnqueueTimes

    val times = "0:50,1:50,2:50,3:50"
    val actual = EventHubsConf.parseEnqueueTimes(times)
    assert(actual == expected)
  }

  test("parseMaxRatesPerPartition") {
    val ehConf = confWithTestValues
    val expected = ehConf.setMaxRatePerPartition(0 to 3, 50).maxRatesPerPartition

    val rates = "0:50,1:50,2:50,3:50"
    val actual = EventHubsConf.parseMaxRatesPerPartition(rates)
    assert(actual == expected)
  }

  // Tests for toMap and toConf
  test("toMap") {
    val ehConf = confWithTestValues
    ehConf.setStartOffsets(0 to 3, 50)
    ehConf.setSqlContainsProperties(true)

    val map = ehConf.toMap
    assert(map("eventhubs.namespace") == ehConf("eventhubs.namespace"))
    assert(map("eventhubs.name") == ehConf("eventhubs.name"))
    assert(map("eventhubs.keyName") == ehConf("eventhubs.keyName"))
    assert(map("eventhubs.key") == ehConf("eventhubs.key"))
    assert(map("eventhubs.partitionCount") == ehConf("eventhubs.partitionCount"))
    assert(map("eventhubs.progressDirectory") == ehConf("eventhubs.progressDirectory"))
    assert(map("eventhubs.consumerGroup") == ehConf("eventhubs.consumerGroup"))
    assert(map("eventhubs.maxRates") == EventHubsConf.maxRatesToString(ehConf.maxRatesPerPartition))
    assert(map("eventhubs.sql.containsProperties") == ehConf("eventhubs.sql.containsProperties"))
    assert(map("eventhubs.startingWith") == "Offsets")
    assert(map("eventhubs.startOffsets") == EventHubsConf.offsetsToString(ehConf.startOffsets))
  }

  test("toConf") {
    val expectedConf =
      confWithTestValues.setStartOffsets(1 to 1, 50).setMaxRatePerPartition(0 to 0, 20)

    val oldSchoolMap = Map[String, String](
      "eventhubs.namespace" -> "namespace",
      "eventhubs.name" -> "name",
      "eventhubs.keyName" -> "keyName",
      "eventhubs.key" -> "key",
      "eventhubs.partitionCount" -> "4",
      "eventhubs.progressDirectory" -> "dir",
      "eventhubs.consumerGroup" -> "consumerGroup",
      "eventhubs.maxRates" -> "0:20",
      "eventhubs.startingWith" -> "Offsets",
      "eventhubs.startOffsets" -> "1:50"
    )
    val actualConf = EventHubsConf.toConf(oldSchoolMap)

    assert(actualConf("eventhubs.namespace") == expectedConf("eventhubs.namespace"))
    assert(actualConf("eventhubs.name") == expectedConf("eventhubs.name"))
    assert(actualConf("eventhubs.keyName") == expectedConf("eventhubs.keyName"))
    assert(actualConf("eventhubs.key") == expectedConf("eventhubs.key"))
    assert(actualConf("eventhubs.partitionCount") == expectedConf("eventhubs.partitionCount"))
    assert(actualConf("eventhubs.progressDirectory") == expectedConf("eventhubs.progressDirectory"))
    assert(actualConf("eventhubs.consumerGroup") == expectedConf("eventhubs.consumerGroup"))
    assert(actualConf.maxRatesPerPartition == expectedConf.maxRatesPerPartition)
    assert(actualConf("eventhubs.startingWith") == "Offsets")
    assert(actualConf.startOffsets == expectedConf.startOffsets)
  }

  test("Map to Conf to Map: there and back again") {
    val oldSchoolMap = Map[String, String](
      "eventhubs.namespace" -> "namespace",
      "eventhubs.name" -> "name",
      "eventhubs.keyName" -> "keyName",
      "eventhubs.key" -> "key",
      "eventhubs.partitionCount" -> "4",
      "eventhubs.progressDirectory" -> "dir",
      "eventhubs.consumerGroup" -> "consumerGroup",
      "eventhubs.maxRates" -> "0:20",
      "eventhubs.startingWith" -> "Offsets",
      "eventhubs.startOffsets" -> "1:50"
    )
    val newMap = EventHubsConf.toConf(oldSchoolMap).toMap
    assert(newMap == oldSchoolMap)
  }

  test("Conf to Map to Conf") {
    val expectedConf = confWithTestValues.setStartOfStream(true)
    val actualConf = EventHubsConf.toConf(expectedConf.toMap)

    assert(actualConf("eventhubs.namespace") == expectedConf("eventhubs.namespace"))
    assert(actualConf("eventhubs.name") == expectedConf("eventhubs.name"))
    assert(actualConf("eventhubs.keyName") == expectedConf("eventhubs.keyName"))
    assert(actualConf("eventhubs.key") == expectedConf("eventhubs.key"))
    assert(actualConf("eventhubs.partitionCount") == expectedConf("eventhubs.partitionCount"))
    assert(actualConf("eventhubs.progressDirectory") == expectedConf("eventhubs.progressDirectory"))
    assert(actualConf("eventhubs.consumerGroup") == expectedConf("eventhubs.consumerGroup"))
    assert(actualConf("eventhubs.startingWith") == expectedConf("eventhubs.startingWith"))
    assert(actualConf.maxRatesPerPartition == expectedConf.maxRatesPerPartition)
    assert(actualConf.startOffsets == expectedConf.startOffsets)
    assert(actualConf.startEnqueueTimes == expectedConf.startEnqueueTimes)
  }
}
