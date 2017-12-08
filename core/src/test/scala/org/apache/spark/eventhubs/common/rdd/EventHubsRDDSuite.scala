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

package org.apache.spark.eventhubs.common.rdd

import org.apache.spark.eventhubs.common.EventHubsConf
import org.apache.spark.eventhubs.common.utils.SimulatedClient
import org.apache.spark.{ SparkConf, SparkContext, SparkFunSuite }
import org.scalatest.BeforeAndAfterAll

class EventHubsRDDSuite extends SparkFunSuite with BeforeAndAfterAll {
  import org.apache.spark.eventhubs.common.utils.EventHubsTestUtils._

  private val sparkConf =
    new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
  private var sc: SparkContext = _

  override def beforeAll {
    sc = new SparkContext(sparkConf)
  }

  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop
      sc = null
    }
  }

  private def getEventHubsConf: EventHubsConf = {
    EventHubsConf()
      .setNamespace("namespace")
      .setName("name")
      .setKeyName("keyName")
      .setKey("key")
      .setConsumerGroup("consumerGroup")
  }

  test("basic usage") {
    val untilSeqNo = 50
    val ehConf = getEventHubsConf

    val offsetRanges = (for {
      partition <- 0 until PartitionCount
    } yield OffsetRange(ehConf.name.get, partition, 0, untilSeqNo)).toArray

    val rdd = new EventHubsRDD(sc, ehConf, offsetRanges, SimulatedClient.apply)
      .map(_.getBytes.map(_.toChar).mkString)

    assert(rdd.count == 50 * PartitionCount)
    assert(!rdd.isEmpty)
  }
}
