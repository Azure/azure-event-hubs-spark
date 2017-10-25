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

import com.microsoft.azure.eventhubs.{ EventData, PartitionReceiver }
import org.apache.spark.SparkConf
import org.apache.spark.eventhubs.common.client.EventHubsClientWrapper
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubDirectDStream

object EventHubsUtils {

  // Will place constants here for now.
  val DefaultMaxRate: String = "10000"
  val DefaultEnqueueTime: String = Long.MinValue.toString
  val StartOfStream = PartitionReceiver.START_OF_STREAM
  val EndOfStream = PartitionReceiver.END_OF_STREAM

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
   * For example, let's say we have a namespace called "test-ns" which had two EventHubs: "eh1" and "eh2".
   * To consume frome both, you need to create a map that maps from those EventHubs names to the EventHubs
   * parameters you'd like to use for each one. For our example, it would be:
   *
   * Map("eh1" -> "~all parameters specific to eh1~",
   *     "eh2" -> "~all parameters specific to eh2~")
   *
   * Pass that Map (of type Map[String, Map[String, String]]) and we'll consume from all EventHubs listed
   *
   * @param ssc the StreamingContext this DStream belongs to
   * @param progressDir progress directory path to store EventHubs specific information(we only support HDFS-based checkpoint
   *                      storage for now, so you have to prefix your path with hdfs://clustername/
   * @param ehParams the parameters of your EventHubs instances
   * @return An EventHubsDirectDStream
   */
  def createDirectStreams(ssc: StreamingContext,
                          progressDir: String,
                          ehParams: Map[String, Map[String, String]]): EventHubDirectDStream = {
    new EventHubDirectDStream(ssc, progressDir, ehParams, EventHubsClientWrapper.apply)
  }

  /**
   * Creates a Direct DStream that consumes from a single Event Hubs instance.
   *
   * @param ssc the StreamingContext this DStream belongs to
   * @param progressDir progress directory path to store EventHubs specific information
   * @param ehParams the parameters of your EventHubs instance
   * @return An EventHubsDirectDStream
   */
  def createDirectStream(ssc: StreamingContext,
                         progressDir: String,
                         ehParams: Map[String, String]): EventHubDirectDStream = {
    val ehName = ehParams("eventhubs.name")
    createDirectStreams(ssc, progressDir, Map(ehName -> ehParams))
  }
}
