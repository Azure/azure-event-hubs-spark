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

import java.time.Duration

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient, PartitionReceiver }
import org.apache.spark.SparkConf
import org.apache.spark.eventhubs.common.client.EventHubsClientWrapper
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubDirectDStream

object EventHubsUtils {

  // Will place constants here for now.
  val DefaultEnqueueTime: String = Long.MinValue.toString
  val StartOfStream = PartitionReceiver.START_OF_STREAM
  val EndOfStream = PartitionReceiver.END_OF_STREAM
  val DefaultMaxRatePerPartition: Rate = 10000
  val DefaultStartOffset: Offset = -1L
  val DefaultReceiverTimeout: Duration = Duration.ofSeconds(5)
  val DefaultOperationTimeout: Duration = Duration.ofSeconds(60)
  val DefaultConsumerGroup: String = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME

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
}
