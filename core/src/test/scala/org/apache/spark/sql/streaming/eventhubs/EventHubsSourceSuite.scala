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

package org.apache.spark.sql.streaming.eventhubs

import java.util.Calendar

import org.scalatest.time.SpanSugar._

import org.apache.spark.eventhubscommon.utils._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext

abstract class EventHubsSourceTest extends StreamTest with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
      super.afterAll()
  }

  override val streamingTimeout: org.scalatest.time.Span = 30.seconds
}

class EventHubsSourceSuite extends EventHubsSourceTest {

  testWithUninterruptibleThread("Verify expected offsets are correct") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"10"
    )

    val eventPayloadsAndProperties = Seq(
      1 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d",
        "propertyE" -> "e", "propertyF" -> "f"),
      0 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j",
        "propertyK" -> "k"),
      3 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o", "propertyP" -> "p"),
      9 -> Seq("propertyQ" -> "q", "propertyR" -> "r", "propertyS" -> "s"),
      5 -> Seq("propertyT" -> "t", "propertyU" -> "u"),
      7 -> Seq("propertyV" -> "v")
    )

    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)

    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)

    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))

    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]

    assert(offset.batchId == 0)
    offset.targetSeqNums.values.foreach(x => assert(x == 2))
  }


  testWithUninterruptibleThread("Verify expected dataframe size is correct") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"10"
    )

    val eventPayloadsAndProperties = Seq(
      1 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d",
        "propertyE" -> "e", "propertyF" -> "f"),
      0 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j",
        "propertyK" -> "k"),
      3 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o", "propertyP" -> "p"),
      9 -> Seq("propertyQ" -> "q", "propertyR" -> "r", "propertyS" -> "s"),
      5 -> Seq("propertyT" -> "t", "propertyU" -> "u"),
      7 -> Seq("propertyV" -> "v")
    )

    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)

    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)

    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))

    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]

    val dataFrame = eventHubsSource.getBatch(None, offset)

    assert(dataFrame.schema == eventHubsSource.schema)

    eventHubsSource.commit(offset)

    assert(dataFrame.select("body").count == 6)
  }


  testWithUninterruptibleThread("Verify user-defined keys show up in dataframe schema") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.sql.containsProperties" -> "true",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"10"
    )

    val eventPayloadsAndProperties = Seq(
      1 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      3 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      5 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      7 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      9 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      11 -> Seq("creationTime" -> Calendar.getInstance().getTime)
    )

    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)

    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)

    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))

    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]

    val dataFrame = eventHubsSource.getBatch(None, offset)

    assert(dataFrame.schema == eventHubsSource.schema)

    eventHubsSource.commit(offset)

    dataFrame.show()

    assert(dataFrame.select("body").count == 6)
  }
}