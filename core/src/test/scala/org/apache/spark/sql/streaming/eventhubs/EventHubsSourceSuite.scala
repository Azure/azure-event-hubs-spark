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
import org.apache.spark.sql.streaming.{EventHubsStreamTest, ProcessingTime}
import org.apache.spark.sql.test.SharedSQLContext

abstract class EventHubsSourceTest extends EventHubsStreamTest with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
      super.afterAll()
  }

  override val streamingTimeout: org.scalatest.time.Span = 30.seconds
}

class EventHubsSourceSuite extends EventHubsSourceTest {

  testWithUninterruptibleThread("Verify expected offsets are correct when rate" +
    " is less than the available data") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"2"
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
    offset.targetSeqNums.values.foreach(x => assert(x == 1))
  }

  testWithUninterruptibleThread("Verify expected offsets are correct when rate" +
    " is more than the available data") {

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

  testWithUninterruptibleThread("Verify expected offsets are correct when" +
    " in subsequent fetch when rate is less than the available data") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"3"
    )

    val eventPayloadsAndProperties = Seq(
      1 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d",
        "propertyE" -> "e", "propertyF" -> "f"),
      2 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d"),
      3 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j",
        "propertyK" -> "k"),
      4 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j"),
      5 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o"),
      6 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o", "propertyP" -> "p"),
      7 -> Seq("propertyQ" -> "q", "propertyR" -> "r", "propertyS" -> "s"),
      8 -> Seq("propertyQ" -> "q", "propertyR" -> "r"),
      9 -> Seq("propertyT" -> "t", "propertyU" -> "u"),
      10 -> Seq("propertyV" -> "v")
    )

    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)

    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)

    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))

    // First batch

    var offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]

    var dataFrame = eventHubsSource.getBatch(None, offset)

    eventHubsSource.commit(offset)

    assert(offset.batchId == 0)
    offset.targetSeqNums.values.foreach(x => assert(x == 2))

    // Second batch

    offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]

    dataFrame = eventHubsSource.getBatch(None, offset)

    eventHubsSource.commit(offset)

    assert(offset.batchId == 1)
    offset.targetSeqNums.values.foreach(x => assert(x == 4))
  }

  testWithUninterruptibleThread("Verify expected dataframe size is correct" +
    " when the rate is less than the available data") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"2"
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

    assert(dataFrame.select("body").count == 4)
  }

  testWithUninterruptibleThread("Verify expected dataframe size is correct" +
    " when the rate is more than the available data") {

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

  testWithUninterruptibleThread("Verify expected dataframe size is correct" +
    " in subsequent fetch when the rate is less than the available data") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"3"
    )

    val eventPayloadsAndProperties = Seq(
      1 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d",
        "propertyE" -> "e", "propertyF" -> "f"),
      2 -> Seq("propertyA" -> "a", "propertyB" -> "b", "propertyC" -> "c", "propertyD" -> "d"),
      3 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j",
        "propertyK" -> "k"),
      4 -> Seq("propertyG" -> "g", "propertyH" -> "h", "propertyI" -> "i", "propertyJ" -> "j"),
      5 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o"),
      6 -> Seq("propertyM" -> "m", "propertyN" -> "n", "propertyO" -> "o", "propertyP" -> "p"),
      7 -> Seq("propertyQ" -> "q", "propertyR" -> "r", "propertyS" -> "s"),
      8 -> Seq("propertyQ" -> "q", "propertyR" -> "r"),
      9 -> Seq("propertyT" -> "t", "propertyU" -> "u"),
      10 -> Seq("propertyV" -> "v")
    )

    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))

    // First batch
    var offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    println(offset)
    var dataFrame = eventHubsSource.getBatch(None, offset)
    dataFrame.show(100)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(dataFrame.select("body").count == 6)

    // Second batch
    offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    println(offset)
    dataFrame = eventHubsSource.getBatch(None, offset)
    dataFrame.show(100)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(dataFrame.select("body").count == 4)
  }

  testWithUninterruptibleThread("Verify user-defined keys show up in dataframe" +
    " schema if specified explicitly") {

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.sql.userDefinedKeys" -> "creationTime",
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

    assert(dataFrame.columns.contains("creationTime"))
  }

  testWithUninterruptibleThread("Verify user-defined keys show up in dataframe" +
    " schema if not specified explicitly") {

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
      1 -> Seq("creationTime" -> Calendar.getInstance().getTime.toString),
      3 -> Seq("creationTime" -> Calendar.getInstance().getTime.toString),
      5 -> Seq("creationTime" -> Calendar.getInstance().getTime.toString),
      7 -> Seq("creationTime" -> Calendar.getInstance().getTime.toString),
      9 -> Seq("creationTime" -> Calendar.getInstance().getTime.toString),
      11 -> Seq("creationTime" -> Calendar.getInstance().getTime.toString)
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

    val properties = dataFrame.select("properties").rdd.map(r => r.get(0)
      .asInstanceOf[Map[String, String]])

    properties.collect().flatMap(x => x.keys).foreach(y => assert(y.equals("creationTime")))

  }

  testWithUninterruptibleThread("Verify dataframe body is correct for String type") {

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
      "A" -> Seq("creationTime" -> Calendar.getInstance().getTime),
      "B" -> Seq("creationTime" -> Calendar.getInstance().getTime),
      "C" -> Seq("creationTime" -> Calendar.getInstance().getTime),
      "D" -> Seq("creationTime" -> Calendar.getInstance().getTime),
      "E" -> Seq("creationTime" -> Calendar.getInstance().getTime),
      "F" -> Seq("creationTime" -> Calendar.getInstance().getTime)
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

    val sparkSession = spark

    import sparkSession.implicits._

    val bodyDataFrame = dataFrame.select("body")
      .map(r => new String(r.getAs[Array[Byte]](0), "UTF-8"))

    val inputArray: Array[String] = eventPayloadsAndProperties.map(x => x._1).toArray
    val outputArray: Array[String] = bodyDataFrame.collect()

    assert(outputArray.sorted.corresponds(inputArray.sorted) {_ == _})
  }

  testWithUninterruptibleThread("Verify dataframe body is correct for Int type") {

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
      2 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      4 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      6 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      8 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      10 -> Seq("creationTime" -> Calendar.getInstance().getTime),
      12 -> Seq("creationTime" -> Calendar.getInstance().getTime)
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

    val sparkSession = spark

    import sparkSession.implicits._

    val bodyDataFrame = dataFrame.select("body")
      .map(r => new String(r.getAs[Array[Byte]](0), "UTF-8").toInt)

    val inputArray: Array[Int] = eventPayloadsAndProperties.map(x => x._1).toArray
    val outputArray: Array[Int] = bodyDataFrame.collect()

    assert(outputArray.sorted.corresponds(inputArray.sorted) {_ == _})
  }

  testWithUninterruptibleThread("Verify expected dataframe can be retrieved through" +
    "StreamingExecution") {

    import testImplicits._

    val eventHubsParameters = Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> "ns1",
      "eventhubs.name" -> "eh1",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "/tmp",
      "eventhubs.maxRate" -> s"3"
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

    val eventHubs: SimulatedEventHubs = EventHubsTestUtilities.simulateEventHubs(
      eventHubsParameters, eventPayloadsAndProperties)

    val dataSource = spark
      .readStream
      .format("eventhubs")
      .options(eventHubsParameters)
      .load()
      .selectExpr("CAST(body AS STRING)")
      .as[(String)]

    val sourceQuery = dataSource.map(x => x.toInt + 1)

    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(0)),
      AddEventHubsData(eventHubsParameters, eventPayloadsAndProperties),
      CheckAnswer(2, 4, 6, 1, 10, 8)
    )

    println()
  }
}