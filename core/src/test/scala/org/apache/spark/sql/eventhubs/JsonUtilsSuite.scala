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

import org.apache.spark.SparkFunSuite
import org.apache.spark.eventhubs.NameAndPartition

class JsonUtilsSuite extends SparkFunSuite {

  test("parsing partitions") {
    val parsed = JsonUtils.partitions("""{"nameA":[0,1],"nameB":[4,6]}""")
    val expected = Array(
      new NameAndPartition("nameA", 0),
      new NameAndPartition("nameA", 1),
      new NameAndPartition("nameB", 4),
      new NameAndPartition("nameB", 6)
    )
    assert(parsed.toSeq === expected.toSeq)
  }

  test("parsing partitionSeqNos") {
    val parsed = JsonUtils.partitionSeqNos("""{"nameA":{"0":23,"1":-1},"nameB":{"0":-2}}""")

    assert(parsed(new NameAndPartition("nameA", 0)) === 23)
    assert(parsed(new NameAndPartition("nameA", 1)) === -1)
    assert(parsed(new NameAndPartition("nameB", 0)) === -2)
  }
}
