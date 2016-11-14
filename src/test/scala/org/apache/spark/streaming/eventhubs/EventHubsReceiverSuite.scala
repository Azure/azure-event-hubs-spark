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

import com.microsoft.azure.eventhubs.EventData.SystemProperties
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.receiver.ReceiverSupervisor
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import com.microsoft.azure.eventhubs._
import org.mockito.internal.util.reflection.Whitebox
import com.microsoft.azure.servicebus.amqp.AmqpConstants

import scala.collection.mutable._

/**
 * Suite of EventHubs streaming receiver tests
 * This suite of tests are low level unit tests, they directly call EventHubsReceiver with mocks
 */
class EventHubsReceiverSuite extends TestSuiteBase with MockitoSugar{
  var eventhubsClientWrapperMock: EventHubsClientWrapper = _
  var offsetStoreMock: OffsetStore = _
  var executorMock: ReceiverSupervisor = _

  val eventhubParameters = Map[String, String] (
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
  }

  override def afterFunction(): Unit = {
    super.afterFunction()
    // Since this suite was originally written using EasyMock, add this to preserve the old
    // mocking semantics (see SPARK-5735 for more details)
    //verifyNoMoreInteractions(ehClientWrapperMock, offsetStoreMock)
  }

  test("EventHubsUtils API works") {
    val streamingContext = new StreamingContext(master, framework, batchDuration)
    EventHubsUtils.createStream(streamingContext, eventhubParameters, "0", StorageLevel.MEMORY_ONLY)
    EventHubsUtils.createUnionStream(streamingContext, eventhubParameters, StorageLevel.MEMORY_ONLY_2)
    streamingContext.stop()
  }

  test("EventHubsReceiver can receive message with proper checkpointing") {

    val eventhubPartitionId: String  = "0"
    val eventCheckpointIntervalInSeconds: Int = 1
    val eventOffset: String = "2147483647"
    val eventSequenceNumber: Long = 1
    val maximumEventRate: Int = 999

    val updatedEventhubsParams = collection.mutable.Map[String, String]() ++= eventhubParameters
    updatedEventhubsParams("eventhubs.checkpoint.interval") = eventCheckpointIntervalInSeconds.toString

    var eventData: EventData = new EventData(Array.fill(8)((scala.util.Random.nextInt(256) - 128).toByte))

    val systemPropertiesMap: java.util.HashMap[String, AnyRef] = new java.util.HashMap[String, AnyRef]()

    systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME, eventOffset)
    systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, Long.box(eventSequenceNumber))
    systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, eventhubPartitionId)

    val systemProperties : SystemProperties =  new SystemProperties(systemPropertiesMap)

    Whitebox.setInternalState(eventData, "systemProperties", systemProperties)

    val eventDataCollection: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()
    eventDataCollection += eventData

    when(offsetStoreMock.read()).thenReturn("-1")
    when(eventhubsClientWrapperMock.receive()).thenReturn(eventDataCollection)

    val receiver = new EventHubsReceiver(updatedEventhubsParams, eventhubPartitionId, StorageLevel.MEMORY_ONLY,
      offsetStoreMock, eventhubsClientWrapperMock, maximumEventRate)

    receiver.attachSupervisor(executorMock)

    receiver.onStart()
    Thread sleep eventCheckpointIntervalInSeconds * 1000
    receiver.onStop()

    verify(offsetStoreMock, times(1)).open()
    verify(offsetStoreMock, times(1)).write(eventOffset)
    verify(offsetStoreMock, times(1)).close()

    verify(eventhubsClientWrapperMock, times(1)).createReceiver(updatedEventhubsParams, eventhubPartitionId,
      offsetStoreMock, maximumEventRate)
    verify(eventhubsClientWrapperMock, atLeastOnce).receive()
    verify(eventhubsClientWrapperMock, times(1)).close()
  }

  test("EventHubsReceiver can restart when exception is thrown") {

    val eventhubPartitionId: String  = "0"
    val eventOffset: String = "2147483647"
    val eventSequenceNumber: Long = 1
    val maximumEventRate: Int = 999

    val eventData: EventData = new EventData(Array.fill(8)((scala.util.Random.nextInt(256) - 128).toByte))

    val systemPropertiesMap: java.util.HashMap[String, AnyRef] = new java.util.HashMap[String, AnyRef]()

    systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME, eventOffset)
    systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, Long.box(eventSequenceNumber))
    systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, eventhubPartitionId)

    val systemProperties : SystemProperties =  new SystemProperties(systemPropertiesMap)

    Whitebox.setInternalState(eventData, "systemProperties", systemProperties)
    val eventDataCollection: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()
    eventDataCollection += eventData

    val eventhubException = new RuntimeException("error")

    when(offsetStoreMock.read()).thenReturn("-1")
    when(eventhubsClientWrapperMock.receive()).thenReturn(eventDataCollection).thenThrow(eventhubException)

    val receiver = new EventHubsReceiver(eventhubParameters, eventhubPartitionId, StorageLevel.MEMORY_ONLY,
      offsetStoreMock, eventhubsClientWrapperMock, maximumEventRate)

    receiver.attachSupervisor(executorMock)

    receiver.onStart()
    Thread sleep 1000
    receiver.onStop()

    verify(executorMock, times(1)).restartReceiver(s"Error handling message," +
      s" restarting receiver for partition $eventhubPartitionId", Some(eventhubException))

    verify(offsetStoreMock, times(1)).open()
    verify(offsetStoreMock, times(1)).close()

    verify(eventhubsClientWrapperMock, times(1)).createReceiver(eventhubParameters, "0", offsetStoreMock,
      maximumEventRate)
    verify(eventhubsClientWrapperMock, times(2)).receive()
    verify(eventhubsClientWrapperMock, times(1)).close()
  }
}