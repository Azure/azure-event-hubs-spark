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

import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition, NameAndPartition }
import org.apache.spark.eventhubs.utils.EventHubsTestUtils
import org.apache.spark.sql.{ DataFrame, QueryTest }
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.BeforeAndAfter

class EventHubsRelationSuite extends QueryTest with BeforeAndAfter with SharedSQLContext {

  import testImplicits._

  private val eventhubId = new AtomicInteger(0)

  private var testUtils: EventHubsTestUtils = _

  private def newEventHub(): String = s"eh-${eventhubId.getAndIncrement()}"

  private def getEventHubsConf(eh: String): EventHubsConf = testUtils.getEventHubsConf(eh)

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

  private def createDF(ehConf: EventHubsConf): DataFrame = {
    spark.read
      .format("eventhubs")
      .options(ehConf.toMap)
      .load()
      .selectExpr("CAST (body AS STRING)")
  }

  private def createPositions(seqNo: Long, ehName: String, partitionCount: Int) = {
    (for {
      p <- 0 until partitionCount
    } yield NameAndPartition(ehName, p) -> EventPosition.fromSequenceNumber(seqNo)).toMap
  }

  test("default earliest to latest events") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 3)
    testUtils.send(eh, 0, 0 to 9)
    testUtils.send(eh, 1, 10 to 19)
    testUtils.send(eh, 2, 20 to 29)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setEndingPositions(Map.empty)

    val df = createDF(ehConf)
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("explicit earliest to latest events") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 3)
    testUtils.send(eh, 0, 0 to 9)
    testUtils.send(eh, 1, 10 to 19)
    testUtils.send(eh, 2, 20 to 29)

    val start = createPositions(0L, eh, partitionCount = 3)
    val end = createPositions(10L, eh, partitionCount = 3)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(start)
      .setEndingPositions(end)

    val df = createDF(ehConf)
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("reuse same dataframe in query") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 1)
    testUtils.send(eh, 0, 0 to 10)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setEndingPositions(Map.empty)

    val df = createDF(ehConf)
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }
}
