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

package org.apache.spark.eventhubs.utils

import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.internal.Logging
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, FunSuite }

/**
 * Tests the functionality of the simulated EventHubs instance used for testing.
 */
class EventHubsTestUtilsSuite
    extends FunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Logging {

  import EventHubsTestUtils._

  private var testUtils: EventHubsTestUtils = _

  override def beforeAll: Unit = {
    testUtils = new EventHubsTestUtils
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.destroyAllEventHubs()
      testUtils = null
    }
  }

  private def getEventHubsConf(name: String): EventHubsConf = testUtils.getEventHubsConf(name)

  private val eventHubsId = new AtomicInteger(0)

  def newEventHubs(): String = {
    s"eh-${eventHubsId.getAndIncrement()}"
  }

  test("Send one event to one partition") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    eventHub.send(0, Seq(0))

    val data = eventHub.getPartitions

    assert(data(0).getEvents.size == 1, "Partition 0 didn't have an event.")

    for (i <- 1 to 3) {
      assert(data(i).getEvents.isEmpty, "Partitions weren't empty")
    }
  }

  test("Send 500 events to all partitions") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 500)

    val data = eventHub.getPartitions

    for (i <- 0 until eventHub.partitionCount) {
      assert(data(i).getEvents.size === 500)
      for (j <- 0 to 499) {
        assert(data(i).get(j).getSystemProperties.getSequenceNumber == j,
               "Sequence number doesn't match expected value.")
      }
    }
  }

  test("All partitions have different data.") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    eventHub.send(0, Seq(1, 2, 3))
    eventHub.send(1, Seq(4, 5, 6))
    eventHub.send(2, Seq(7, 8, 9))
    eventHub.send(3, Seq(10, 11, 12))

    val data = eventHub.getPartitions

    assert(data(0).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(1, 2, 3))
    assert(data(1).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(4, 5, 6))
    assert(data(2).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(7, 8, 9))
    assert(data(3).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(10, 11, 12))
  }

  test("translate") {
    val eh = newEventHubs()
    testUtils.createEventHubs(eh, DefaultPartitionCount)
    val conf = getEventHubsConf(eh)
    val client = SimulatedClient(conf)

    val actual = client.translate(conf, client.partitionCount)
    val expected = conf.startingPositions.get.map { case (k, v) => k.partitionId -> v.seqNo }

    assert(actual === expected)
  }

  test("Test simulated receiver") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 500)

    val data = eventHub.getPartitions

    for (i <- 0 until eventHub.partitionCount) {
      assert(data(i).getEvents.size === 500)
      for (j <- 0 to 499) {
        assert(data(i).get(j).getSystemProperties.getSequenceNumber == j,
               "Sequence number doesn't match expected value.")
      }
    }

    val conf = testUtils.getEventHubsConf(eventHub.name)
    val client = SimulatedClient(conf)
    client.createReceiver(partitionId = "0", 20)
    val event = client.receive(1)
    assert(event.iterator.next.getSystemProperties.getSequenceNumber === 20)
  }

  test("latestSeqNo") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)

    eventHub.send(0, Seq(1))
    eventHub.send(1, Seq(2, 3))
    eventHub.send(2, Seq(4, 5, 6))
    eventHub.send(3, Seq(7))

    val conf = testUtils.getEventHubsConf(eventHub.name)
    val client = SimulatedClient(conf)
    assert(client.latestSeqNo(0) == 1)
    assert(client.latestSeqNo(1) == 2)
    assert(client.latestSeqNo(2) == 3)
    assert(client.latestSeqNo(3) == 1)
  }

  test("partitionSize") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)

    assert(eventHub.partitionSize(0) == 0)
    assert(eventHub.partitionSize(1) == 0)
    assert(eventHub.partitionSize(2) == 0)
    assert(eventHub.partitionSize(3) == 0)

    eventHub.send(0, Seq(1))
    eventHub.send(1, Seq(2, 3))
    eventHub.send(2, Seq(4, 5, 6))
    eventHub.send(3, Seq(7))

    assert(eventHub.partitionSize(0) == 1)
    assert(eventHub.partitionSize(1) == 2)
    assert(eventHub.partitionSize(2) == 3)
    assert(eventHub.partitionSize(3) == 1)
  }

  test("totalSize") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)

    assert(eventHub.totalSize == 0)

    eventHub.send(0, Seq(1))
    eventHub.send(1, Seq(2, 3))
    eventHub.send(2, Seq(4, 5, 6))
    eventHub.send(3, Seq(7))

    assert(eventHub.totalSize == 7)
  }

  test("send EventData") {
    // events are sent round-robin, so the first event will go to partition 0.
    val part = 0

    val eh = newEventHubs()
    testUtils.createEventHubs(eh, partitionCount = 10)

    val ehConf = getEventHubsConf(eh)
    val client = new SimulatedClient(ehConf)
    val event = EventData.create("1".getBytes)
    client.send(event)

    assert(testUtils.getEventHubs(eh).getPartitions(part).size == 1)
    assert(
      testUtils
        .getEventHubs(eh)
        .getPartitions(part)
        .getEvents
        .head
        .getBytes
        .sameElements(event.getBytes))

  }

  test("send EventData to specific partition") {
    // use this partition in the partition sender
    val part = 7

    val eh = newEventHubs()
    testUtils.createEventHubs(eh, partitionCount = 10)

    val ehConf = getEventHubsConf(eh)
    val client = new SimulatedClient(ehConf)
    val event = EventData.create("1".getBytes)
    client.send(event, part)

    assert(testUtils.getEventHubs(eh).getPartitions(part).size == 1)
    assert(
      testUtils
        .getEventHubs(eh)
        .getPartitions(part)
        .getEvents
        .head
        .getBytes
        .sameElements(event.getBytes))

  }
}
