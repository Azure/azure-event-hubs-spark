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

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventhubs.EventData.SystemProperties
import com.microsoft.azure.eventhubs.amqp.AmqpConstants
import org.mockito.internal.util.reflection.Whitebox
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.eventhubscommon.client.{EventHubsClientWrapper, EventHubsOffsetTypes}
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore
import org.apache.spark.util.Utils


/**
 * Test suite for ReliableEventHubsReceiver
 * This suite of tests use Spark local mode with EventHubs dummy receiver for e2e testing
 */
class ReliableEventHubsReceiverSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll
    with MockitoSugar with Eventually {
  private var streamingContext: StreamingContext = _
  private var ehClientWrapperMock: EventHubsClientWrapper = _
  private var offsetStoreMock: OffsetStore = _
  private var tempDirectory: File = _

  private val eventhubParameters = Map[String, String] (
    "eventhubs.policyname" -> "policyname",
    "eventhubs.policykey" -> "policykey",
    "eventhubs.namespace" -> "namespace",
    "eventhubs.name" -> "name",
    "eventhubs.partition.count" -> "4",
    "eventhubs.checkpoint.dir" -> "checkpointdir",
    "eventhubs.checkpoint.interval" -> "0"
  )

  private val sparkConf = new SparkConf()
    .setMaster("local[3]") // At least 2, 1 for receiver and 1 for data transform
    .setAppName("ReliableEventHubsReceiverSuite")
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    .set("spark.driver.allowMultipleContexts", "true")

  override def beforeAll() : Unit = {

  }

  override def afterAll() : Unit = {
  }

  before {
    tempDirectory = Utils.createTempDir()
    // tempDirectory.deleteOnExit()
    streamingContext = new StreamingContext(sparkConf, Milliseconds(500))
    streamingContext.checkpoint(tempDirectory.getAbsolutePath)

    offsetStoreMock = new MyMockedOffsetStore
  }

  after {
    if (streamingContext != null) {
      streamingContext.stop()
      streamingContext = null
    }
    if(tempDirectory != null) {
      // Utils.deleteRecursively(tempDirectory)
      tempDirectory.delete()
      tempDirectory = null
    }
  }

  // Test ignored due to an issue with mocking library unavailable to the executors.

  test("Reliable EventHubs input stream") {
    // after 100 messages then start to receive null
    ehClientWrapperMock = new MyMockedEventHubsClientWrapper(100, -1)
    val stream = EventHubsUtils.createStream(streamingContext, eventhubParameters, "0",
      StorageLevel.MEMORY_ONLY, offsetStoreMock, ehClientWrapperMock)
    var count = 0
    stream.map { v => v }.foreachRDD { r =>
      val ret = r.collect()
      ret.foreach { v =>
        count += 1
      }
    }
    streamingContext.start()
    eventually(timeout(4000.milliseconds), interval(200.milliseconds)) {
      // Make sure we have received 100 messages
      assert(count === 100)
    }
  }

  test("Reliable EventHubs input stream recover from exception") {
    // After 60 messages then exception, after 100 messages then receive null
    ehClientWrapperMock = new MyMockedEventHubsClientWrapper(100, 60)
    val stream = EventHubsUtils.createStream(streamingContext, eventhubParameters, "0",
      StorageLevel.MEMORY_ONLY, offsetStoreMock, ehClientWrapperMock)
    var count = 0
    stream.map { v => v }.foreachRDD { r =>
      val ret = r.collect()
      ret.foreach { v =>
        count += 1
      }
    }
    streamingContext.start()

    eventually(timeout(10000.milliseconds), interval(200.milliseconds)) {

      // Make sure we have received 100 messages
      assert(count === 100)
    }
  }
}

/**
 * The Mock class for EventHubsClientWrapper.
 * Note this class only support offset filter.
 *
 * @param emitCount the number of message emitted before it returns null
 * @param exceptionCount the number of message emitted before it throws exception
 *                       it only throws exception once
 */
class MyMockedEventHubsClientWrapper(
    emitCount: Int,
    exceptionCount: Int) extends EventHubsClientWrapper with MockitoSugar {
  var offset: Int = -1
  var count = 0
  var partition = "0"
  var myExceptionCount: Int = exceptionCount

  override def createReceiverInternal(
      connectionString: String,
      eventhubsName: String,
      consumerGroup: String,
      partitionId: String,
      offsetType: EventHubsOffsetType,
      currentOffset: String,
      receiverEpoch: Long): Unit = {
    if (offsetType != EventHubsOffsetTypes.None) {
      offset = currentOffset.toInt
      partition = partitionId
    }
  }

  override def closeReceiver(): Unit = {
    // no ops
  }

  override def receive(): Iterable[EventData] = {

    if (count == myExceptionCount) {
      // make sure we only throw exception once
      myExceptionCount = -1
      throw new RuntimeException("count = " + count)
    }
    offset += 1
    count += 1
    // do not send more than emitCount number of messages
    if(count <= emitCount) {

      val eventData = new EventData(Array.fill(8)(
        (scala.util.Random.nextInt(256) - 128).toByte))
      val systemPropertiesMap = new java.util.HashMap[String, AnyRef]()
      systemPropertiesMap.put(AmqpConstants.OFFSET_ANNOTATION_NAME, offset.toString)
      systemPropertiesMap.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, Long.box(count))
      systemPropertiesMap.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, partition)
      val systemProperties = new SystemProperties(systemPropertiesMap)
      Whitebox.setInternalState(eventData, "systemProperties", systemProperties)
      val eventDataCollection: ArrayBuffer[EventData] = new ArrayBuffer[EventData]()
      eventDataCollection += eventData
      eventDataCollection
    } else {
      Thread sleep 1000
      null
    }
  }
}

/**
 * The Mock class for OffsetStore
 */
class MyMockedOffsetStore extends OffsetStore {
  var myOffset: String = "-1"
  override def open(): Unit = {
  }

  override def write(offset: String): Unit = {
    println("writing offset to MyMockedOffsetStore:" + offset)
    myOffset = offset
  }

  override def read(): String = {
    println("reading offset from MyMockedOffsetStore:" + myOffset)
    myOffset
  }

  override def close(): Unit = {
  }
}
