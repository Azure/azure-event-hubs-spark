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
package org.apache.spark.eventhubs.common

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.eventhubs.common.client.EventHubsClientWrapper
import org.apache.spark.eventhubs.common.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubDirectDStream

object EventHubsUtils {

  /**
   * Return an initialized SparkConf that registered
   * Azure Eventhubs client's internal classes with Kryo serializer
   *
   * @return SparkConf
   */
  def initializeSparkStreamingConfigurations: SparkConf = {
    new SparkConf().registerKryoClasses(Array(classOf[EventData]))
  }

  /**
   * Creates a Direct DStream that consumes from multiple Event Hubs instances within a namespace.
   *
   * @param ssc the StreamingContext this DStream belongs to
   * @param ehConf the parameters for your EventHubs instance
   * @return An EventHubsDirectDStream
   */
  def createDirectStream(ssc: StreamingContext, ehConf: EventHubsConf): EventHubDirectDStream = {
    new EventHubDirectDStream(ssc, ehConf, EventHubsClientWrapper.apply)
  }

  def createRDD(sc: SparkContext,
                ehConf: EventHubsConf,
                offsetRanges: Array[OffsetRange]): EventHubsRDD = {
    new EventHubsRDD(sc, ehConf, offsetRanges, EventHubsClientWrapper.apply)
  }
}
