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

import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite

class EventPositionSuite extends AnyFunSuite {

  test("convert - offset") {
    val actual = EventPosition.fromOffset("123456789").convert
    val expected = com.microsoft.azure.eventhubs.EventPosition.fromOffset("123456789", true)
    assert(actual.toString === expected.toString)
  }

  test("convert - seq no") {
    val actual = EventPosition.fromSequenceNumber(42L).convert
    val expected = com.microsoft.azure.eventhubs.EventPosition.fromSequenceNumber(42L, true)
    assert(actual.toString === expected.toString)
  }

  test("convert - enqueued time") {
    val instant = Instant.parse("2007-12-03T10:15:30.00Z")
    val actual = EventPosition.fromEnqueuedTime(instant).convert
    val expected = com.microsoft.azure.eventhubs.EventPosition.fromEnqueuedTime(instant)
    assert(actual.toString === expected.toString)
  }

  test("convert - start of stream") {
    val actual = EventPosition.fromStartOfStream.convert
    val expected = com.microsoft.azure.eventhubs.EventPosition.fromStartOfStream()
    assert(actual.toString === expected.toString)
  }

  test("convert - end of stream") {
    val actual = EventPosition.fromEndOfStream.convert
    val expected = com.microsoft.azure.eventhubs.EventPosition.fromEndOfStream()
    assert(actual.toString === expected.toString)
  }
}
