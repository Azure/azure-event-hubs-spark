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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.utils.EventHubsTestUtils
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.streaming.{ DataStreamWriter, OutputMode, StreamTest, StreamingQuery }
import org.apache.spark.sql.test.SharedSQLContext

class EventHubsSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._
  import EventHubsTestUtils._

  protected var testUtils: EventHubsTestUtils = _

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

  private def createEventHubsReader(ehConf: EventHubsConf): DataFrame = {
    spark.read
      .format("eventhubs")
      .options(ehConf.toMap)
      .load()
  }

  private def createEventHubsWriter(
      input: DataFrame,
      ehConf: EventHubsConf,
      withOutputMode: Option[OutputMode] = None)(withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.nonEmpty) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("eventhubs")
        .options(ehConf.toMap)
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .queryName("kafkaStream")
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

    checkAnswer(createEventHubsReader(ehConf).select("body"),
                Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }
}
