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

import org.apache.spark.eventhubs.utils.EventHubsTestUtils
import org.scalatest.FunSuite

/**
 * Tests [[EventHubsConf]] for correctness.
 */
class EventHubsConfSuite extends FunSuite {

  // Return an EventHubsConf with dummy data
  private def confWithTestValues: EventHubsConf = {
    EventHubsConf(EventHubsTestUtils.ConnectionString)
      .setConsumerGroup("consumerGroup")
  }

  // Tests for get, set, and isValid
  test("set throws NullPointerException for null key and value") {
    val ehConf = confWithTestValues
    intercept[NullPointerException] { ehConf.set(null, "value") }
    intercept[NullPointerException] { ehConf.set("key", null) }
    intercept[NullPointerException] { ehConf.set(null, null) }
  }

  test("set/apply/get are properly working") {
    val ehConf = confWithTestValues.set("some key", "some value")
    assert(ehConf("some key") == "some value")
  }

  // TODO revist isValid
  ignore("isValid doesn't return true until all required data is provided") {
    val ehConf = confWithTestValues
  }
}
