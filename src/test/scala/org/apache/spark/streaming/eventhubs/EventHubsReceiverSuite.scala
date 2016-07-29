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

//import java.util.concurrent.CompletableFuture

import com.microsoft.azure.eventhubs.EventData.SystemProperties
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.receiver.ReceiverSupervisor
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import com.microsoft.azure.eventhubs._
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner

import scala.collection.mutable._

/**
 * Suite of EventHubs streaming receiver tests
 * This suite of tests are low level unit tests, they directly call EventHubsReceiver with mocks
 */
@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[SystemProperties]))
class EventHubsReceiverSuite extends TestSuiteBase with MockitoSugar{
  var eventhubsClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  var executorMock: ReceiverSupervisor = _
  var eventDataCollectionMock: ArrayBuffer[EventData] = _
  var eventDataMock: EventData = _
  //var systemPropertiesMock: SystemProperties = _

  val ehParams = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "1000"
  )

  override def beforeFunction() = {

    eventhubsClientWrapperMock = mock[EventHubsClientWrapper]
    offsetStoreMock = mock[OffsetStore]
    executorMock = mock[ReceiverSupervisor]
    //completableFutureEventDataMock = mock[CompletableFuture[java.lang.Iterable[EventData]]]
    eventDataCollectionMock = mock[ArrayBuffer[EventData]]
    eventDataMock = mock[EventData]
    //systemPropertiesMock = mock[SystemProperties]

    /*
    eventhubsClientWrapperMock = PowerMockito.mock[EventHubsClientWrapper](classOf[EventHubsClientWrapper])
    offsetStoreMock = PowerMockito.mock[OffsetStore](classOf[OffsetStore])
    executorMock = PowerMockito.mock[ReceiverSupervisor](classOf[ReceiverSupervisor])
    eventDataCollectionMock = PowerMockito.mock[ArrayBuffer[EventData]](classOf[ArrayBuffer[EventData]])
    eventDataMock = PowerMockito.mock[EventData](classOf[EventData])
    systemPropertiesMock = PowerMockito.mock[SystemProperties](classOf[SystemProperties])
    */
  }

  override def afterFunction(): Unit = {
    super.afterFunction()
    // Since this suite was originally written using EasyMock, add this to preserve the old
    // mocking semantics (see SPARK-5735 for more details)
    //verifyNoMoreInteractions(ehClientWrapperMock, offsetStoreMock)
  }

  test("EventHubsUtils API works") {
    val streamingContext = new StreamingContext(master, framework, batchDuration)
    EventHubsUtils.createStream(streamingContext, ehParams, "0", StorageLevel.MEMORY_ONLY)
    EventHubsUtils.createUnionStream(streamingContext, ehParams, StorageLevel.MEMORY_ONLY_2)
    streamingContext.stop()
  }

  // To be revisited. Sequence Number of Event Data is part of System Properties which is final in Eventhubs Client
  // and cannot be mocked.

  ignore("EventHubsReceiver can receive message with proper checkpointing") {
    val ehParams2 = collection.mutable.Map[String, String]() ++= ehParams
    ehParams2("eventhubs.checkpoint.interval") = "10"

    // Mock object setup

    //val eventDataCollection: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()

    eventDataMock = new EventData(Array[Byte](1,2,3,4), 2147483647, 4)

    eventDataCollectionMock += eventDataMock

    Mockito.when(offsetStoreMock.read()).thenReturn("-1")

    //when(systemPropertiesMock.getSequenceNumber).thenReturn(8)
    //when(eventDataMock.getSystemProperties).thenReturn(systemPropertiesMock)
    //when(eventDataMock.getSequenceNumber).thenReturn(8)

    PowerMockito.mockStatic(classOf[SystemProperties])

    //PowerMockito.when(systemPropertiesMock.getSequenceNumber).thenReturn(8)

    //PowerMockito.when(classOf[EventData], eventDataMock.getSystemProperties.getSequenceNumber).thenReturn(8)

    //PowerMockito.when(eventDataMock.getSystemProperties.getSequenceNumber).thenReturn(8)

    Mockito.when(eventhubsClientWrapperMock.receive()).thenReturn(eventDataCollectionMock).thenReturn(null)

    val receiver = new EventHubsReceiver(ehParams2, "0", StorageLevel.MEMORY_ONLY, offsetStoreMock,
      eventhubsClientWrapperMock, 999)

    receiver.attachSupervisor(executorMock)

    receiver.onStart()
    Thread sleep (100)
    receiver.onStop()

    verify(offsetStoreMock, times(1)).open()
    verify(offsetStoreMock, times(1)).write("2147483647")
    verify(offsetStoreMock, times(1)).close()

    verify(eventhubsClientWrapperMock, times(1)).createReceiver(ehParams2, "0", offsetStoreMock, 999)
    verify(eventhubsClientWrapperMock, atLeastOnce).receive()
    verify(eventhubsClientWrapperMock, times(1)).close()
  }

  // To be revisited. Sequence Number of Event Data is part of System Properties which is final in Eventhubs Client
  // and cannot be mocked.

  ignore("EventHubsReceiver can restart when exception is thrown") {
    // Mock object setup

    val exception = new RuntimeException("error")

    when(offsetStoreMock.read()).thenReturn("-1")

    val eventDataCollection: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()

    eventDataCollection += new EventData(Array[Byte](1,2,3,4), 2147483647, 4)

    eventDataCollection += eventDataMock

    when(eventhubsClientWrapperMock.receive()).thenReturn(eventDataCollection).thenThrow(exception)

    val receiver = new EventHubsReceiver(ehParams, "0", StorageLevel.MEMORY_ONLY, offsetStoreMock,
      eventhubsClientWrapperMock, 999)

    receiver.attachSupervisor(executorMock)

    receiver.onStart()
    Thread sleep (1000)
    receiver.onStop()

    // Verify that executor.restartReceiver() has been called
    verify(executorMock, times(1)).restartReceiver("Error handling message, restarting receiver", Some(exception))

    verify(offsetStoreMock, times(1)).open()
    verify(offsetStoreMock, times(1)).close()

    verify(eventhubsClientWrapperMock, times(1)).createReceiver(ehParams, "0", offsetStoreMock, 999)
    verify(eventhubsClientWrapperMock, times(2)).receive()
    verify(eventhubsClientWrapperMock, times(1)).close()
  }
}