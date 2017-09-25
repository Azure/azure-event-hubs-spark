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
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubscommon.utils._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.util.Utils

class EventHubsSourceSuite extends EventHubsStreamTest {

  private def buildEventHubsParamters(
      namespace: String,
      name: String,
      partitionCount: Int,
      maxRate: Int,
      containsProperties: Boolean = false,
      userDefinedKeys: Option[String] = None,
      enqueueTime: Option[Long] = None): Map[String, String] = {
    Map[String, String](
      "eventhubs.policyname" -> "policyName",
      "eventhubs.policykey" -> "policyKey",
      "eventhubs.namespace" -> s"$namespace",
      "eventhubs.name" -> s"$name",
      "eventhubs.partition.count" -> s"$partitionCount",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> tempRoot,
      "eventhubs.maxRate" -> s"$maxRate",
      "eventhubs.sql.containsProperties" -> s"$containsProperties"
    ) ++ userDefinedKeys.map(udk => Map("eventhubs.sql.userDefinedKeys" -> udk)).getOrElse(Map()) ++
      enqueueTime.map(et => Map("eventhubs.filter.enqueuetime" -> et.toString)).getOrElse(Map())
  }

  private def generateIntKeyedData(num: Int, offset: Int = 0): Seq[(Int, Seq[(String, String)])] = {
    for (bodyId <- 0 until num)
      yield (bodyId + offset) -> Seq("propertyV" -> "v")
  }

  private def generateStringKeyedData(num: Int): Seq[(String, Seq[(String, String)])] = {
    for (bodyId <- 0 until num)
      yield bodyId.toString -> Seq("propertyV" -> "v")
  }

  private def generateKeyedDataWithNullValue(num: Int): Seq[(String, Seq[(String, String)])] = {
    for (bodyId <- 0 until num)
      yield bodyId.toString -> Seq("propertyV" -> null, "property" -> null)
  }

  test("Verify expected offsets are correct when rate is less than the available data") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 2)
    val eventPayloadsAndProperties = generateIntKeyedData(6).map{case (body, properties) =>
      (body.asInstanceOf[Int], properties)}
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    assert(offset.batchId == 0)
    offset.targetSeqNums.values.foreach(x => assert(x == 1))
  }

  test("Verify expected offsets are correct when rate is more than the available data") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10)
    val eventPayloadsAndProperties = generateIntKeyedData(6)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       offsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          EventHubsOffsetTypes.PreviousCheckpoint),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    assert(offset.batchId == 0)
    offset.targetSeqNums.values.foreach(x => assert(x == 2))
  }

  test("Verify expected offsets are correct when in subsequent fetch when rate is less than the" +
    " available data") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties = generateIntKeyedData(10)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    // First batch
    var offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    var dataFrame = eventHubsSource.getBatch(None, offset)
    dataFrame.foreach(_ => Unit)
    eventHubsSource.commit(offset)
    assert(offset.batchId == 0)
    offset.targetSeqNums.values.foreach(x => assert(x == 2))
    // Second batch
    offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    dataFrame = eventHubsSource.getBatch(None, offset)
    dataFrame.foreach(_ => Unit)
    eventHubsSource.commit(offset)
    assert(offset.batchId == 1)
    offset.targetSeqNums.values.foreach(x => assert(x == 4))
  }

  test("Verify expected dataframe size is correct when the rate is less than the available data") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 2)
    val eventPayloadsAndProperties = generateIntKeyedData(6)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    val dataFrame = eventHubsSource.getBatch(None, offset)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(dataFrame.select("body").count == 4)
  }

  test("Verify expected dataframe size is correct when the rate is more than the available data") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10)
    val eventPayloadsAndProperties = generateIntKeyedData(6)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    val dataFrame = eventHubsSource.getBatch(None, offset)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(dataFrame.select("body").count == 6)
  }

  test("Verify expected dataframe size is correct in subsequent fetch when the rate is" +
    " less than the available data") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties = generateIntKeyedData(10)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    // First batch
    var offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    var dataFrame = eventHubsSource.getBatch(None, offset)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(dataFrame.select("body").count == 6)
    // Second batch
    offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    dataFrame = eventHubsSource.getBatch(None, offset)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(dataFrame.select("body").count == 4)
  }

  test("Verify all user-defined keys show up in dataframe schema if not specify" +
    " userDefinedKeys") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10,
      containsProperties = true)
    val eventPayloadsAndProperties = Seq(
      1 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      3 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      5 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      7 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      9 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      11 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1)
    )
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    val dataFrame = eventHubsSource.getBatch(None, offset)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    val properties = dataFrame.select("properties").rdd.map(r => r.get(0)
      .asInstanceOf[Map[String, String]])
    assert(properties.collect().forall(propertyMap => propertyMap.keySet == Set("creationTime",
      "otherUserDefinedKey")))
  }

  test("Verify user-defined keys show up in dataframe schema if specify userDefinedKey") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10,
      containsProperties = true, userDefinedKeys = Some("otherUserDefinedKey,"))
    val eventPayloadsAndProperties = Seq(
      1 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      3 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      5 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      7 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      9 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1),
      11 -> Seq("creationTime" -> Calendar.getInstance().getTime, "otherUserDefinedKey" -> 1)
    )
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       ehOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          ehOffsetType),
      (_: String, _: Map[String, Map[String, String]]) =>
        new TestRestEventHubClient(highestOffsetPerPartition))
    val offset = eventHubsSource.getOffset.get.asInstanceOf[EventHubsBatchRecord]
    val dataFrame = eventHubsSource.getBatch(None, offset)
    assert(dataFrame.schema == eventHubsSource.schema)
    eventHubsSource.commit(offset)
    assert(!dataFrame.columns.contains("creationTime"))
    assert(dataFrame.columns.contains("otherUserDefinedKey"))
  }

  test("Verify null references in user-defined keys are handled correctly") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10,
      containsProperties = true)
    val eventPayloadsAndProperties = generateKeyedDataWithNullValue(6)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
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
    val inputArray = eventPayloadsAndProperties.map(x => x._1).toArray
    val outputArray = bodyDataFrame.collect()
    assert(outputArray.sorted.corresponds(inputArray.sorted) {_ == _})
  }

  test("Verify dataframe body is correct for String type") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10)
    val eventPayloadsAndProperties = generateStringKeyedData(6)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
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
    val inputArray = eventPayloadsAndProperties.map(x => x._1).toArray
    val outputArray = bodyDataFrame.collect()
    assert(outputArray.sorted.corresponds(inputArray.sorted) {_ == _})
  }

  test("Verify dataframe body is correct for Int type") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 10)
    val eventPayloadsAndProperties = generateIntKeyedData(6)
    val eventHubs = EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val eventHubsSource = new EventHubsSource(spark.sqlContext, eventHubsParameters,
      (eventHubsParams: Map[String, String], partitionId: Int, startOffset: Long,
       eventHubsOffsetType: EventHubsOffsetType, _: Int) =>
        new TestEventHubsReceiver(eventHubsParams, eventHubs, partitionId, startOffset,
          eventHubsOffsetType),
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
    val inputArray = eventPayloadsAndProperties.map(x => x._1).toArray
    val outputArray = bodyDataFrame.collect()
    assert(outputArray.sorted.corresponds(inputArray.sorted) {_ == _})
  }

  private def generateInputQuery(
      eventHubsParams: Map[String, String],
      sparkSession: SparkSession): Dataset[_] = {
    import sparkSession.implicits._
    val dataSource = spark
      .readStream
      .format("eventhubs")
      .options(eventHubsParams)
      .load()
      .selectExpr("CAST(body AS STRING)")
      .as[String]
    dataSource.map(x => x.toInt + 1)
  }

  test("Verify input row metric is correct when source" +
    " is started with initial data") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties = generateIntKeyedData(6)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(0)),
      AddEventHubsData(eventHubsParameters),
      CheckAnswer(1, 3, 5, 2, 4, 6),
      AssertOnQuery { sourceQuery =>
        val recordsRead = sourceQuery.recentProgress.map(_.numInputRows).sum
        recordsRead == 6
      }
    )
  }

  test("Verify expected dataframe can be retrieved after data addition to source") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties = generateIntKeyedData(6)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val highestBatchId = 1
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      CheckAnswer(),
      AddEventHubsData(eventHubsParameters, highestBatchId, eventPayloadsAndProperties),
      AdvanceManualClock(10),
      CheckAnswer(1, 3, 5, 2, 4, 6)
    )
  }

  test("Verify expected dataframe can be retrieved after data added to source in excess" +
    " of the rate") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties = generateIntKeyedData(15)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val highestBatchId = 3
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      CheckAnswer(),
      AddEventHubsData(eventHubsParameters, highestBatchId, eventPayloadsAndProperties),
      AdvanceManualClock(10),
      AdvanceManualClock(10),
      AdvanceManualClock(10),
      CheckAnswer(1, 3, 5, 7, 9, 11, 13, 15, 2, 4, 6, 8, 10, 12, 14)
    )
  }

  test("Verify expected dataframe can be retrieved when more data is added to" +
    " source after stream has started") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties1 = generateIntKeyedData(6)
    val eventPayloadsAndProperties2 = generateIntKeyedData(6, offset = 2)
    val eventPayloadsAndProperties3 = generateIntKeyedData(6, offset = 3)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties1)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val highestBatchId = new AtomicInteger(0)
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      AddEventHubsData(eventHubsParameters),
      CheckAnswer(1, 3, 5, 2, 4, 6),
      AddEventHubsData(eventHubsParameters, highestBatchId.incrementAndGet.toLong,
        eventPayloadsAndProperties2),
      AdvanceManualClock(10),
      CheckAnswer(1, 3, 5, 2, 4, 6, 3, 5, 7, 4, 6, 8),
      AddEventHubsData(eventHubsParameters, highestBatchId.incrementAndGet.toLong,
        eventPayloadsAndProperties3),
      AdvanceManualClock(10),
      CheckAnswer(1, 3, 5, 2, 4, 6, 3, 5, 7, 4, 6, 8, 4, 6, 8, 5, 7, 9)
    )
  }

  test("Verify expected dataframe can be retrieved with data added to source after the stream" +
    " has started") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties1 = generateIntKeyedData(6)
    val eventPayloadsAndProperties2 = generateIntKeyedData(6, offset = 2)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val highestBatchId = new AtomicInteger(0)
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      CheckAnswer(),
      AddEventHubsData(eventHubsParameters, highestBatchId.incrementAndGet().toLong,
        eventPayloadsAndProperties1),
      AdvanceManualClock(10),
      CheckAnswer(1, 3, 5, 2, 4, 6),
      AddEventHubsData(eventHubsParameters, highestBatchId.incrementAndGet().toLong,
        eventPayloadsAndProperties2),
      AdvanceManualClock(10),
      CheckAnswer(1, 3, 5, 2, 4, 6, 3, 5, 7, 4, 6, 8)
    )
  }

  test("Verify expected dataframe can be retrieved from different" +
    " sources with same event hubs on different streams on different queries at same rate") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 30)
    val eventPayloadsAndProperties = generateIntKeyedData(1000)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties)
    val sourceQuery1 = generateInputQuery(eventHubsParameters, spark)
    val sourceQuery2 = generateInputQuery(eventHubsParameters, spark)
    testStream(sourceQuery1)(
      StartStream(trigger = ProcessingTime(200)),
      AddEventHubsData(eventHubsParameters, highestBatchId = 16),
      CheckAnswer(1 to 1000: _*)
    )
    testStream(sourceQuery2)(
      StartStream(trigger = ProcessingTime(200)),
      AddEventHubsData(eventHubsParameters, highestBatchId = 16),
      CheckAnswer(1 to 1000: _*)
    )
  }

  test("Verify expected dataframe can be retrieved from different " +
    "sources with same event hubs on different streams on different queries at different rates") {
    import testImplicits._
    val eventHubsParameters1 = buildEventHubsParamters("ns1", "eh1", 2, 30)
    val eventHubsParameters2 = buildEventHubsParamters("ns1", "eh1", 2, 10)
    val eventPayloadsAndProperties = generateIntKeyedData(1000)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters1, eventPayloadsAndProperties)
    val sourceQuery1 = generateInputQuery(eventHubsParameters1, spark)
    val sourceQuery2 = generateInputQuery(eventHubsParameters2, spark)
    testStream(sourceQuery1)(
      StartStream(trigger = ProcessingTime(200)),
      AddEventHubsData(eventHubsParameters1, highestBatchId = 16),
      CheckAnswer(1 to 1000: _*)
    )
    testStream(sourceQuery2)(
      StartStream(trigger = ProcessingTime(200)),
      AddEventHubsData(eventHubsParameters2, highestBatchId = 49),
      CheckAnswer(1 to 1000: _*)
    )
  }

  test("Verify expected dataframe can be retrieved from same " +
    "source on different queries") {
    import testImplicits._
    val eventHubsParameters1 = buildEventHubsParamters("ns1", "eh1", 2, 30)
    val eventPayloadsAndProperties = generateIntKeyedData(1000)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters1, eventPayloadsAndProperties)
    val sourceQuery = generateInputQuery(eventHubsParameters1, spark)
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(200)),
      AddEventHubsData(eventHubsParameters1, highestBatchId = 16),
      CheckAnswer(1 to 1000: _*)
    )
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(200)),
      AddEventHubsData(eventHubsParameters1, highestBatchId = 16),
      CheckAnswer(1 to 1000: _*)
    )
  }

  test("Verify expected dataframe can be retrieved when the stream is stopped before the last" +
    " batch's offset is committed") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 30)
    val eventPayloadsAndProperties = generateIntKeyedData(1000)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties.take(30 * 10 * 2))
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock(0)
    val firstBatch = Seq(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      AddEventHubsData(eventHubsParameters, 9))
    val clockMove = Array.fill(9)(AdvanceManualClock(10)).toSeq
    val secondBatch = Seq(
      CheckAnswer(1 to 600: _*),
      StopStream(recoverStreamId = true, commitPartialOffset = true, partialType = "delete"),
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock,
        additionalConfs = Map("eventhubs.test.newSink" -> "true")),
      AddEventHubsData(eventHubsParameters, 17, eventPayloadsAndProperties.takeRight(400)))
    val clockMove2 = Array.fill(8)(AdvanceManualClock(10)).toSeq
    val thirdBatch = Seq(CheckAnswer(541 to 1000: _*))
    testStream(sourceQuery)(firstBatch ++ clockMove ++ secondBatch ++ clockMove2 ++ thirdBatch: _*)
  }

  test("Verify expected dataframe can be retrieved when the stream is stopped after the last" +
    " batch's offset is committed") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 30)
    val eventPayloadsAndProperties = generateIntKeyedData(1000)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties.take(30 * 10 * 2))
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val firstBatch = Seq(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      AddEventHubsData(eventHubsParameters, 9))
    val clockMove = Array.fill(9)(AdvanceManualClock(10)).toSeq
    val secondBatch = Seq(
      CheckAnswer(1 to 600: _*),
      StopStream(recoverStreamId = true, commitOffset = true),
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock,
        additionalConfs = Map("eventhubs.test.newSink" -> "true")),
      AddEventHubsData(eventHubsParameters, 17, eventPayloadsAndProperties.takeRight(400)))
    val clockMove2 = Array.fill(8)(AdvanceManualClock(10)).toSeq
    val thirdBatch = Seq(CheckAnswer(601 to 1000: _*))
    testStream(sourceQuery)(firstBatch ++ clockMove ++ secondBatch ++ clockMove2 ++ thirdBatch: _*)
  }

  test("Verify expected dataframe can be retrieved when metadata is not committed") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 30)
    val eventPayloadsAndProperties = generateIntKeyedData(1000)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties.take(30 * 10 * 2))
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val firstBatch = Seq(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      AddEventHubsData(eventHubsParameters, 9))
    val clockMove = Array.fill(9)(AdvanceManualClock(10)).toSeq
    val secondBatch = Seq(
      CheckAnswer(1 to 600: _*),
      StopStream(recoverStreamId = true, commitPartialOffset = true,
        partialType = "deletemetadata"),
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock,
        additionalConfs = Map("eventhubs.test.newSink" -> "true")),
      AddEventHubsData(eventHubsParameters, 17, eventPayloadsAndProperties.takeRight(400)))
    val clockMove2 = Array.fill(8)(AdvanceManualClock(10)).toSeq
    val thirdBatch = Seq(CheckAnswer(541 to 1000: _*))
    testStream(sourceQuery)(firstBatch ++ clockMove ++ secondBatch ++ clockMove2 ++ thirdBatch: _*)
  }

  test("Verify expected dataframe is retrieved from starting offset" +
    " on different streams on the same query") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3)
    val eventPayloadsAndProperties1 = generateIntKeyedData(6)
    val eventPayloadsAndProperties2 = generateIntKeyedData(12)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties1)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    val highestBatchId = new AtomicInteger(0)
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      AddEventHubsData(eventHubsParameters, highestBatchId.incrementAndGet().toLong),
      AdvanceManualClock(10),
      CheckAnswer(1, 2, 3, 4, 5, 6),
      StopStream(),
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock,
        additionalConfs = Map(
          "eventhubs.test.checkpointLocation" ->
            s"${Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath}",
          "eventhubs.test.newSink" -> "true")),
      AddEventHubsData(eventHubsParameters),
      CheckAnswer(1, 2, 3, 4, 5, 6),
      AddEventHubsData(eventHubsParameters, highestBatchId.incrementAndGet().toLong,
        eventPayloadsAndProperties2),
      AdvanceManualClock(10),
      AdvanceManualClock(10),
      CheckAnswer(1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    )
  }

  private def testDataForWindowingOperation(batchSize: Int, creationTime: Long) = {
    for (bodyId <- 0 until batchSize)
      yield bodyId -> Seq("creationTime" -> s"1999-12-31 00:00:$creationTime")
  }

  test("Verify expected dataframe is retrieved with windowing operation") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 40,
      containsProperties = true, userDefinedKeys = Some("creationTime"))
    val eventPayloadsAndProperties = {
      for (time <- Range(0, 10))
        yield testDataForWindowingOperation(100, time)
    }.reduce((a, b) => a ++ b)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val sourceQuery = spark.readStream.format("eventhubs").options(eventHubsParameters).load()
    import sourceQuery.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val windowedStream = sourceQuery.groupBy(
      window(
        $"creationTime".cast(TimestampType),
        "3 second",
        "1 second")).count().sort("window").select("count")
    val manualClock = new StreamManualClock
    val firstBatch = Seq(StartStream(trigger = ProcessingTime(1000), triggerClock = manualClock))
    val clockMove = Array.fill(13)(AdvanceManualClock(1000)).toSeq
    val secondBatch = Seq(
      AddEventHubsData(eventHubsParameters, 12),
      CheckAnswer(true, 100, 200, 300, 300, 300, 300, 300, 300, 300, 300, 200, 100))
    testStream(windowedStream, outputMode = OutputMode.Complete())(
      firstBatch ++ clockMove ++ secondBatch: _*)
  }

  private def testDataForWatermark(batchSize: Int) = {
    val normalData = {
      for (ct <- 0 until 20)
        yield 1 -> Seq("creationTime" -> s"1999-12-31 00:00:$ct")
    }
    val lateData = {
      for (ct <- 15 until 25)
        yield 1 -> Seq("creationTime" -> s"1999-12-31 00:00:$ct")
    }
    val rejectData = {
      for (ct <- 10 until 15)
        yield 1 -> Seq("creationTime" -> s"1999-12-31 00:00:$ct")
    }
    normalData ++ lateData ++ rejectData
  }

  test("Verify expected dataframe is retrieved with watermarks") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 1, 1,
      containsProperties = true, userDefinedKeys = Some("creationTime"))
    val eventPayloadsAndProperties = testDataForWatermark(2)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters,
      eventPayloadsAndProperties)
    val sourceQuery = spark.readStream.format("eventhubs").options(eventHubsParameters).load()
    import sourceQuery.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val windowedStream = sourceQuery.selectExpr(
      "CAST(creationTime AS TIMESTAMP) as creationTimeT").
      withWatermark("creationTimeT", "5 second").
      groupBy(window($"creationTimeT", "3 second", "1 second")).
      count().select("count")
    val manualClock = new StreamManualClock
    val firstBatch = Seq(StartStream(trigger = ProcessingTime(1000), triggerClock = manualClock))
    val clockMove = Array.fill(35)(AdvanceManualClock(1000)).toSeq
    val secondBatch = Seq(
      AddEventHubsData(eventHubsParameters, 35),
      CheckAnswer(true, 1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 5, 6, 6))
    testStream(windowedStream, outputMode = OutputMode.Append())(
      firstBatch ++ clockMove ++ secondBatch: _*)
  }

  test("Filter enqueuetime correctly in structured streaming") {
    import testImplicits._
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3, enqueueTime = Some(3000))
    val eventPayloadsAndProperties = generateIntKeyedData(15)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      AddEventHubsData(eventHubsParameters, 2),
      UpdatePartialCheck(
        EventHubsBatchRecord(0,
          Map(EventHubNameAndPartition("eh1", 1) -> 2, EventHubNameAndPartition("eh1", 0) -> 2))),
      CheckAnswer(true, false, 7, 8, 9, 10, 11, 12),
      // in the second batch we have the right seq number of msgs
      UpdatePartialCheck(
        EventHubsBatchRecord(1,
          Map(EventHubNameAndPartition("eh1", 1) -> 6, EventHubNameAndPartition("eh1", 0) -> 7))),
      AdvanceManualClock(10),
      CheckAnswer(true, false, 7, 8, 9, 10, 11, 12, 13, 14, 15)
    )
  }

  test("Users cannot submit enqueueTime which is later than the latest in the queue") {
    val eventHubsParameters = buildEventHubsParamters("ns1", "eh1", 2, 3,
      enqueueTime = Some(Long.MaxValue))
    val eventPayloadsAndProperties = generateIntKeyedData(15)
    EventHubsTestUtilities.simulateEventHubs(eventHubsParameters, eventPayloadsAndProperties)
    val sourceQuery = generateInputQuery(eventHubsParameters, spark)
    val manualClock = new StreamManualClock
    testStream(sourceQuery)(
      StartStream(trigger = ProcessingTime(10), triggerClock = manualClock),
      ExpectFailure[IllegalArgumentException]()
    )
  }
}
