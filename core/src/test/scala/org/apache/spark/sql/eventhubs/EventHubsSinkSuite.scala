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

package org.apache.spark.sql.eventhubs

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.time.SpanSugar._

import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.utils.EventHubsTestUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.time.Span

class EventHubsSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._
  import EventHubsTestUtils._

  protected var testUtils: EventHubsTestUtils = _

  override val streamingTimeout: Span = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new EventHubsTestUtils
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.destroyAllEventHubs()
      testUtils = null
      super.afterAll()
    }
  }

  private val eventHubId = new AtomicInteger(0)

  private def newEventHub(): String = s"eh-${eventHubId.getAndIncrement}"

  private def getEventHubsConf(name: String) = testUtils.getEventHubsConf(name)

  private def createReader(ehConf: EventHubsConf): DataFrame = {
    spark.read
      .format("eventhubs")
      .options(ehConf.toMap)
      .load()
      .select($"body" cast "string")
  }

  private def createEventHubsWriter(
      input: DataFrame,
      ehConf: EventHubsConf,
      withOutputMode: Option[OutputMode] = None)(withSelectExrp: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF().withColumnRenamed("value", "body")
      if (withSelectExrp.nonEmpty) {
        df = df.selectExpr(withSelectExrp: _*)
      }
      stream = df.writeStream
        .format("eventhubs")
        .options(ehConf.toMap)
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .queryName("eventHubStream")
      withOutputMode.foreach(stream.outputMode(_))
    }
    stream.start()
  }

  test("batch - write to EventHubs") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, DefaultPartitionCount)
    val ehConf = getEventHubsConf(eh)
    val df = Seq("1", "2", "3", "4", "5").toDF("body")

    df.write
      .format("eventhubs")
      .options(ehConf.toMap)
      .save()

    checkAnswer(createReader(ehConf),
                Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("batch - write to specific partition id") {
    val eh = newEventHub()
    val targetPartition = "0"
    testUtils.createEventHubs(eh, DefaultPartitionCount)

    val ehConf = getEventHubsConf(eh)
    val df = Seq("1", "2", "3", "4", "5").map(v => (targetPartition, v)).toDF("partitionId", "body")

    df.write
      .format("eventhubs")
      .options(ehConf.toMap)
      .save()

    assert(testUtils.getEventHubs(eh).getPartitions(targetPartition.toInt).size == 5)
    checkAnswer(createReader(ehConf),
                Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("batch - unsupported save modes") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, DefaultPartitionCount)
    val ehConf = getEventHubsConf(eh)
    val df = Seq[(String, String)](("0", "1")).toDF("partitionId", "body")

    // Test bad save mode Ignore
    var ex = intercept[AnalysisException] {
      df.write
        .format("eventhubs")
        .options(ehConf.toMap)
        .mode(SaveMode.Ignore)
        .save()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains(s"save mode ignore not allowed for eventhubs"))

    // Test bad save mode Overwrite
    ex = intercept[AnalysisException] {
      df.write
        .format("eventhubs")
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains(s"save mode overwrite not allowed for eventhubs"))
  }

  test("SPARK-20496: batch - enforce analyzed plans") {
    val inputEvents =
      spark
        .range(1, 1000)
        .select(to_json(struct("*")) as 'body)

    val eh = newEventHub()
    testUtils.createEventHubs(eh, DefaultPartitionCount)
    val ehConf = getEventHubsConf(eh)
    // Should not throw UnresolvedException
    inputEvents.write
      .format("eventhubs")
      .options(ehConf.toMap)
      .save()
  }

  test("streaming - write to eventhubs") {
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, DefaultPartitionCount)
    val ehConf = getEventHubsConf(eh)

    val writer = createEventHubsWriter(
      input.toDF,
      ehConf,
      withOutputMode = Some(OutputMode.Append)
    )("body")

    val reader = (e: EventHubsConf) => createReader(e).as[String].map(_.toInt)

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader(ehConf), 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader(ehConf), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write to specific partition") {
    val targetPart = "0"
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 10)
    val ehConf = getEventHubsConf(eh)

    val writer = createEventHubsWriter(
      input.toDF,
      ehConf,
      withOutputMode = Some(OutputMode.Append())
    )(s"'$targetPart' as partitionId", "body")

    val reader = (e: EventHubsConf) => createReader(e).as[String].map(_.toInt)

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      assert(testUtils.getEventHubs(eh).getPartitions(targetPart.toPartitionId).size == 6)
      checkDatasetUnorderly(reader(ehConf), 1, 2, 2, 3, 3, 3)
      input.addData("1", "2", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      assert(testUtils.getEventHubs(eh).getPartitions(targetPart.toPartitionId).size == 9)
      checkDatasetUnorderly(reader(ehConf), 1, 2, 2, 3, 3, 3, 1, 2, 3)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write data with bad schema - no body field") {
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 10)
    val ehConf = getEventHubsConf(eh)

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createEventHubsWriter(input.toDF(), ehConf)("body as foo")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("required attribute 'body' not found."))
  }

  test("streaming - write data with bad schema - partitionKey and partitionId have been set") {
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 10)
    val ehConf = getEventHubsConf(eh)

    var writer: StreamingQuery = null
    var ex: Exception = null
    val partitionKey = "foo"
    val partitionId = "0"
    try {
      ex = intercept[StreamingQueryException] {
        writer = createEventHubsWriter(input.toDF(), ehConf)(s"'$partitionKey' as partitionKey",
                                                             s"'$partitionId' as partitionId",
                                                             "body")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains(
          s"both a partitionkey ($partitionKey) and partitionid ($partitionId) have been detected. both can not be set."))
  }

  test("streaming - write data with valid schema but wrong type - bad body type") {
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 10)
    val ehConf = getEventHubsConf(eh)

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createEventHubsWriter(input.toDF(), ehConf)("CAST (body as INT) as body")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains("body attribute type must be a string or binarytype"))
  }

  test("streaming - write data with valid schema but wrong type - bad partitionId type") {
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 10)
    val ehConf = getEventHubsConf(eh)

    var writer: StreamingQuery = null
    var ex: Exception = null
    val partitionId = "0"
    try {
      ex = intercept[StreamingQueryException] {
        writer =
          createEventHubsWriter(input.toDF(), ehConf)(s"CAST('$partitionId' as INT) as partitionId",
                                                      "body")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains(s"partitionid attribute unsupported type"))
  }

  test("streaming - write data with valid schema but wrong type - bad partitionKey type") {
    val input = MemoryStream[String]
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 10)
    val ehConf = getEventHubsConf(eh)

    var writer: StreamingQuery = null
    var ex: Exception = null
    val partitionKey = "234"
    try {
      ex = intercept[StreamingQueryException] {
        writer = createEventHubsWriter(input.toDF(), ehConf)(
          s"CAST('$partitionKey' as INT) as partitionKey",
          "body")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains(s"partitionkey attribute unsupported type"))
  }
}
