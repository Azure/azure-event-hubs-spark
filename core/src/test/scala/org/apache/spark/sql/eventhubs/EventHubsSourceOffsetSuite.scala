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

import java.io.File

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OffsetSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.Assertions._

class EventHubsSourceOffsetSuite extends OffsetSuite with SharedSparkSession {

  compare(one = EventHubsSourceOffset(("t", 0, 1L)), two = EventHubsSourceOffset(("t", 0, 2L)))

  compare(one = EventHubsSourceOffset(("t", 0, 1L), ("t", 1, 0L)),
          two = EventHubsSourceOffset(("t", 0, 2L), ("t", 1, 1L)))

  compare(one = EventHubsSourceOffset(("t", 0, 1L), ("T", 0, 0L)),
          two = EventHubsSourceOffset(("t", 0, 2L), ("T", 0, 1L)))

  compare(one = EventHubsSourceOffset(("t", 0, 1L)),
          two = EventHubsSourceOffset(("t", 0, 2L), ("t", 1, 1L)))

  val ehso1 = EventHubsSourceOffset(("t", 0, 1L))
  val ehso2 = EventHubsSourceOffset(("t", 0, 2L), ("t", 1, 3L))
  val ehso3 = EventHubsSourceOffset(("t", 0, 2L), ("t", 1, 3L), ("t", 1, 4L))

  compare(EventHubsSourceOffset(SerializedOffset(ehso1.json)),
          EventHubsSourceOffset(SerializedOffset(ehso2.json)))

  test("basic serialization - deserialization") {
    assert(
      EventHubsSourceOffset.getPartitionSeqNos(ehso1) ==
        EventHubsSourceOffset.getPartitionSeqNos(SerializedOffset(ehso1.json)))
  }

  test("OffsetSeqLog serialization - deserialization") {
    withTempDir { temp =>
      // use non-existent directory to test whether log make the dir
      val dir = new File(temp, "dir")
      val metadataLog = new OffsetSeqLog(spark, dir.getAbsolutePath)
      val batch0 = OffsetSeq.fill(ehso1)
      val batch1 = OffsetSeq.fill(ehso2, ehso3)

      val batch0Serialized =
        OffsetSeq.fill(batch0.offsets.flatMap(_.map(o => SerializedOffset(o.json))): _*)

      val batch1Serialized =
        OffsetSeq.fill(batch1.offsets.flatMap(_.map(o => SerializedOffset(o.json))): _*)

      assert(metadataLog.add(0, batch0))
      assert(metadataLog.getLatest() === Some(0 -> batch0Serialized))
      assert(metadataLog.get(0) === Some(batch0Serialized))

      assert(metadataLog.add(1, batch1))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(
        metadataLog.get(None, Some(1)) ===
          Array(0 -> batch0Serialized, 1 -> batch1Serialized))

      // Adding the same batch does nothing
      metadataLog.add(1, OffsetSeq.fill(LongOffset(3)))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(
        metadataLog.get(None, Some(1)) ===
          Array(0 -> batch0Serialized, 1 -> batch1Serialized))
    }
  }

  test("read Spark 2.1.0 offset format") {
    val offset = readFromResource("eventhubs-source-offset-version-2.1.0.txt")
    assert(
      EventHubsSourceOffset(offset) ===
        EventHubsSourceOffset(("ehName1", 0, 456L), ("ehName1", 1, 789L), ("ehName2", 0, 0L)))
  }

  private def readFromResource(file: String): SerializedOffset = {
    import scala.io.Source
    val input = getClass.getResource(s"/$file").toURI
    val str = Source.fromFile(input).mkString
    SerializedOffset(str)
  }
}
