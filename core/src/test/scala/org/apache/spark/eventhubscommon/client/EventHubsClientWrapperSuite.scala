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
package org.apache.spark.eventhubscommon.client

import org.scalatest.{ BeforeAndAfter, FunSuite }
import org.scalatest.mock.MockitoSugar

import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore

/**
 * Test suite for EventHubsClientWrapper
 */
class EventHubsClientWrapperSuite extends FunSuite with BeforeAndAfter with MockitoSugar {
  var ehClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  val ehParams: Map[String, String] = Map(
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4"
  )

  before {
    offsetStoreMock = mock[OffsetStore]
  }

  // TODO: re-implement these tests. The previous tests were pointless after the client redesign.
  test("EventHubsClientWrapper converts parameters correctly when offset was previously saved") {}

  test("EventHubsClientWrapper converts parameters for consumergroup") {}

  test("EventHubsClientWrapper converts parameters for enqueuetime filter") {}
}
