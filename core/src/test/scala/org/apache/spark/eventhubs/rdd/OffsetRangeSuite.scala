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

package org.apache.spark.eventhubs.rdd

import org.scalatest.FunSuite

class OffsetRangeSuite extends FunSuite {
  test("offset range - toString") {
    val ehName = "eh-0"
    val fromSeqNo = 0
    val untilSeqNo = 50

    val offsetRanges = (for {
      partition <- 0 until 5
    } yield OffsetRange(ehName, partition, fromSeqNo, untilSeqNo, None)).toArray

    assert(
      offsetRanges.map(_.toString) === Array(
        "OffsetRange(name: eh-0 | partition: 0 | fromSeqNo: 0 | untilSeqNo: 50)",
        "OffsetRange(name: eh-0 | partition: 1 | fromSeqNo: 0 | untilSeqNo: 50)",
        "OffsetRange(name: eh-0 | partition: 2 | fromSeqNo: 0 | untilSeqNo: 50)",
        "OffsetRange(name: eh-0 | partition: 3 | fromSeqNo: 0 | untilSeqNo: 50)",
        "OffsetRange(name: eh-0 | partition: 4 | fromSeqNo: 0 | untilSeqNo: 50)"
      ))
  }
}
