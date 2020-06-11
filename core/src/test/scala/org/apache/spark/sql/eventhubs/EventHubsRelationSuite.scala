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

import org.apache.qpid.proton.amqp._
import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition, NameAndPartition }
import org.apache.spark.eventhubs.utils.EventHubsTestUtils
import org.apache.spark.sql.{ DataFrame, QueryTest }
import org.apache.spark.sql.test.SharedSQLContext
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.scalatest.BeforeAndAfter

class EventHubsRelationSuite extends QueryTest with BeforeAndAfter with SharedSQLContext {

  import testImplicits._

  private val eventhubId = new AtomicInteger(0)

  private var testUtils: EventHubsTestUtils = _

  implicit val formats = Serialization.formats(NoTypeHints)

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
      .select($"body" cast "string")
  }

  private def createPositions(seqNo: Long, ehName: String, partitionCount: Int) = {
    (for {
      p <- 0 until partitionCount
    } yield NameAndPartition(ehName, p) -> EventPosition.fromSequenceNumber(seqNo)).toMap
  }

  test("default earliest to latest events") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 3)
    testUtils.send(eh, partition = Some(0), data = 0 to 9)
    testUtils.send(eh, partition = Some(1), data = 10 to 19)
    testUtils.send(eh, partition = Some(2), data = 20 to 29)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setEndingPositions(Map.empty)

    val df = createDF(ehConf)
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("explicit earliest to latest events") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 3)
    testUtils.send(eh, partition = Some(0), data = 0 to 9)
    testUtils.send(eh, partition = Some(1), data = 10 to 19)
    testUtils.send(eh, partition = Some(2), data = 20 to 29)

    val start = createPositions(0L, eh, partitionCount = 3)
    val end = createPositions(10L, eh, partitionCount = 3)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(start)
      .setEndingPositions(end)

    val df = createDF(ehConf)
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("with application properties") {
    val properties: Option[Map[String, Object]] = Some(
      Map(
        "A" -> "Hello, world.",
        "B" -> Map.empty,
        "C" -> "432".getBytes,
        "D" -> null,
        "E" -> Boolean.box(true),
        "F" -> Int.box(1),
        "G" -> Long.box(1L),
        "H" -> Char.box('a'),
        "I" -> new Binary("1".getBytes),
        "J" -> Symbol.getSymbol("x-opt-partition-key"),
        "K" -> new Decimal128(Array[Byte](0, 1, 2, 3, 0, 0, 0, 0, 0, 1, 2, 3, 0, 0, 0, 0)),
        "L" -> new Decimal32(12),
        "M" -> new Decimal64(13),
        "N" -> new UnsignedByte(1.toByte),
        "O" -> new UnsignedLong(987654321L),
        "P" -> new UnsignedShort(Short.box(1)),
        "Q" -> new UnknownDescribedType("descriptor", "described")
      ))

    // The expected serializes to:
    //     [Map(E -> true, N -> "1", J -> "x-opt-partition-key", F -> 1, A -> Hello, world.,
    //     M -> 13, I -> [49], G -> 1, L -> 12, B -> {}, P -> "1", C -> [52,51,50], H -> "a",
    //     K -> [0,1,2,3,0,0,0,0,0,1,2,3,0,0,0,0], O -> "987654321", D -> null)]
    val expected = properties.get
      .mapValues {
        case b: Binary =>
          val buf = b.asByteBuffer()
          val arr = new Array[Byte](buf.remaining)
          buf.get(arr)
          arr.asInstanceOf[AnyRef]
        case d128: Decimal128    => d128.asBytes.asInstanceOf[AnyRef]
        case d32: Decimal32      => d32.getBits.asInstanceOf[AnyRef]
        case d64: Decimal64      => d64.getBits.asInstanceOf[AnyRef]
        case s: Symbol           => s.toString.asInstanceOf[AnyRef]
        case ub: UnsignedByte    => ub.toString.asInstanceOf[AnyRef]
        case ui: UnsignedInteger => ui.toString.asInstanceOf[AnyRef]
        case ul: UnsignedLong    => ul.toString.asInstanceOf[AnyRef]
        case us: UnsignedShort   => us.toString.asInstanceOf[AnyRef]
        case c: Character        => c.toString.asInstanceOf[AnyRef]
        case d: DescribedType    => d.getDescribed
        case default             => default
      }
      .mapValues {
        case s: String => s
        case default   => Serialization.write(default)
      }

    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 3)
    testUtils.send(eh, Some(0), 0 to 9, properties)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setEndingPositions(Map.empty)

    val df = spark.read
      .format("eventhubs")
      .options(ehConf.toMap)
      .load()
      .select("properties")

    checkAnswer(df, Seq.fill(10)(expected).toDF)
  }

  test("reuse same dataframe in query") {
    val eh = newEventHub()
    testUtils.createEventHubs(eh, partitionCount = 1)
    testUtils.send(eh, partition = Some(0), data = 0 to 10)

    val ehConf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setEndingPositions(Map.empty)

    val df = createDF(ehConf)
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }
}
