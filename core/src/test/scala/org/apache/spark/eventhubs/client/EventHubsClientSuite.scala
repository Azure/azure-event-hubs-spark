/**
 *  Here we need to test all functionality against an EventHubs instance.
 *  Add a testing context with EventHub info, and then make calls against that instance.
 */
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

package org.apache.spark.eventhubs.client

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, FunSuite }

/**
 * Test suite for EventHubsClient
 */
class EventHubsClientSuite extends FunSuite with BeforeAndAfter with MockitoSugar {
  // TODO: add tests for driver-side translation
  // Seems we can implement "translate" in our EventHubsTestUtils. I'll do that soon!

  test("EventHubsClient converts parameters correctly when offset was previously saved") {}

  test("EventHubsClient converts parameters for consumergroup") {}

  test("EventHubsClient converts parameters for enqueuetime filter") {}
}
