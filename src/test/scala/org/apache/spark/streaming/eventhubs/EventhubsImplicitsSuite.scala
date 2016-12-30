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

import org.scalatest.mock.MockitoSugar

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore
import org.apache.spark.streaming.receiver.ReceiverSupervisor

class EventhubsImplicitsSuite
  extends TestSuiteBase with org.scalatest.Matchers with MockitoSugar {

  var ehClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  var executorMock: ReceiverSupervisor = _
  val ehParams = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "1000"
  )

  test("StreamingContext can be implicitly converted to eventhub streaming context") {
    val ssc = new StreamingContext(master, framework, batchDuration)

    import org.apache.spark.streaming.eventhubs.Implicits._

    val stream = ssc.unionedEventHubStream(ehParams)
    val stream2 = ssc.eventHubStream(ehParams, "0")
    ssc.stop()
  }
}
