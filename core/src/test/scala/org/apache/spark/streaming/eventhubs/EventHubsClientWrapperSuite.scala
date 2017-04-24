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

import com.microsoft.azure.eventhubs._
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.streaming.eventhubs.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore

/**
 * Test suite for EventHubsClientWrapper
 */
class EventHubsClientWrapperSuite extends FunSuite with BeforeAndAfter with MockitoSugar {
  var ehClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  val ehParams = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "0"
  )

  before {
    ehClientWrapperMock = spy(new EventHubsClientWrapper)
    offsetStoreMock = mock[OffsetStore]
  }

  test("EventHubsClientWrapper converts parameters correctly when offset was previously saved") {
    Mockito.when(offsetStoreMock.read()).thenReturn("2147483647")
    Mockito.doNothing().when(ehClientWrapperMock).createReceiverInternal(
      Matchers.anyString,
      Matchers.anyString,
      Matchers.anyString,
      Matchers.eq[EventHubsOffsetType](EventHubsOffsetTypes.PreviousCheckpoint),
      Matchers.anyString,
      Matchers.anyLong)

    ehClientWrapperMock.createReceiver(ehParams, "4", offsetStoreMock, 999)

    verify(ehClientWrapperMock, times(1)).createReceiverInternal(
      Matchers.eq("Endpoint=amqps://namespace.servicebus.windows.net;EntityPath=name;" +
        "SharedAccessKeyName=policyname;" +
        "SharedAccessKey=policykey;OperationTimeout=PT1M;RetryPolicy=Default"),
      Matchers.eq(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME),
      Matchers.eq("4"),
      Matchers.eq(EventHubsOffsetTypes.PreviousCheckpoint),
      Matchers.eq("2147483647"),
      Matchers.eq(-1L))
  }

  test("EventHubsClientWrapper converts parameters for consumergroup") {
    var ehParams2 = ehParams
    ehParams2 += "eventhubs.consumergroup" -> "$consumergroup"
    when(offsetStoreMock.read()).thenReturn("-1")
    doNothing().when(ehClientWrapperMock).createReceiverInternal(Matchers.anyString,
      Matchers.anyString,
      Matchers.anyString,
      Matchers.eq[EventHubsOffsetType](EventHubsOffsetTypes.None),
      Matchers.anyString,
      Matchers.anyLong)
    ehClientWrapperMock.createReceiver(ehParams2, "4", offsetStoreMock, 999)
    verify(ehClientWrapperMock, times(1)).createReceiverInternal(
      Matchers.eq("Endpoint=amqps://namespace.servicebus.windows.net;EntityPath=name;" +
        "SharedAccessKeyName=policyname;" +
        "SharedAccessKey=policykey;OperationTimeout=PT1M;RetryPolicy=Default"),
      Matchers.eq("$consumergroup"),
      Matchers.eq("4"),
      Matchers.eq(EventHubsOffsetTypes.None),
      Matchers.eq("-1"),
      Matchers.eq(-1L))
  }

  test("EventHubsClientWrapper converts parameters for enqueuetime filter") {
    var ehParams2 = ehParams
    ehParams2 += "eventhubs.filter.enqueuetime" -> "1433887583"
    when(offsetStoreMock.read()).thenReturn("-1")
    doNothing().when(ehClientWrapperMock).createReceiverInternal(
      Matchers.anyString,
      Matchers.anyString,
      Matchers.anyString,
      Matchers.eq[EventHubsOffsetType](EventHubsOffsetTypes.InputTimeOffset),
      Matchers.anyString,
      Matchers.anyLong)

    ehClientWrapperMock.createReceiver(ehParams2, "4", offsetStoreMock, 999)

    verify(ehClientWrapperMock, times(1)).createReceiverInternal(
      Matchers.eq("Endpoint=amqps://namespace.servicebus.windows.net;EntityPath=name;" +
        "SharedAccessKeyName=policyname;" +
        "SharedAccessKey=policykey;OperationTimeout=PT1M;RetryPolicy=Default"),
      Matchers.eq(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME),
      Matchers.eq("4"),
      Matchers.eq(EventHubsOffsetTypes.InputTimeOffset),
      Matchers.eq("1433887583"),
      Matchers.eq(-1L))
  }
}
