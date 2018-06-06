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

package org.apache.spark.eventhubs

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.client.EventHubsClient
import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.eventhubs.EventHubsDirectDStream
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.streaming.api.java.{ JavaInputDStream, JavaStreamingContext }

/**
 * Helper to create Direct DStreams which consume events from Event Hubs.
 */
object EventHubsUtils {

  /**
   * Creates a Direct DStream which consumes from  the Event Hubs instance
   * specified in the [[EventHubsConf]].
   *
   * @param ssc the StreamingContext this DStream belongs to
   * @param ehConf the parameters for your EventHubs instance
   * @return An [[EventHubsDirectDStream]]
   */
  def createDirectStream(ssc: StreamingContext, ehConf: EventHubsConf): EventHubsDirectDStream = {
    new EventHubsDirectDStream(ssc, ehConf, EventHubsClient.apply)
  }

  /**
   * Creates a Direct DStream which consumes from  the Event Hubs instance
   * specified in the [[EventHubsConf]].
   *
   * @param jssc the JavaStreamingContext this DStream belongs to
   * @param ehConf the parameters for your EventHubs instance
   * @return A [[JavaInputDStream]] containing [[EventData]]
   */
  def createDirectStream(jssc: JavaStreamingContext,
                         ehConf: EventHubsConf): JavaInputDStream[EventData] = {
    new JavaInputDStream(createDirectStream(jssc.ssc, ehConf))
  }

  /**
   * Creates an RDD which is contains events from an EventHubs instance.
   * Starting and ending offsets are specified in advance.
   *
   * @param sc the SparkContext the RDD belongs to
   * @param ehConf contains EventHubs-specific configurations
   * @param offsetRanges offset ranges that define the EventHubs data belonging to this RDD
   * @return An [[EventHubsRDD]]
   *
   */
  def createRDD(sc: SparkContext,
                ehConf: EventHubsConf,
                offsetRanges: Array[OffsetRange]): EventHubsRDD = {
    new EventHubsRDD(sc, EventHubsConf.trim(ehConf), offsetRanges)
  }

  /**
   * Creates an RDD which is contains events from an EventHubs instance.
   * Starting and ending offsets are specified in advance.
   *
   * @param jsc the JavaSparkContext the RDD belongs to
   * @param ehConf contains EventHubs-specific configurations
   * @param offsetRanges offset ranges that define the EventHubs data belonging to this RDD
   * @return A [[JavaRDD]] containing [[EventData]]
   *
   */
  def createRDD(jsc: JavaSparkContext,
                ehConf: EventHubsConf,
                offsetRanges: Array[OffsetRange]): JavaRDD[EventData] = {
    new JavaRDD(createRDD(jsc.sc, ehConf, offsetRanges))
  }
}
