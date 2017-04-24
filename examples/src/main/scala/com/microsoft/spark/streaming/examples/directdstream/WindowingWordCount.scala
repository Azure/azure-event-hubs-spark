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

package com.microsoft.spark.streaming.examples.directdstream

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

object WindowingWordCount {

  private def createStreamingContext(
      sparkCheckpointDir: String,
      batchDuration: Int,
      namespace: String,
      eventHunName: String,
      eventhubParams: Map[String, String],
      progressDir: String) = {
    val ssc = new StreamingContext(new SparkContext(), Seconds(batchDuration))
    ssc.checkpoint(sparkCheckpointDir)
    val inputDirectStream = EventHubsUtils.createDirectStreams(
      ssc,
      namespace,
      progressDir,
      Map(eventHunName -> eventhubParams))

    inputDirectStream.map(receivedRecord => (new String(receivedRecord.getBody), 1)).
      reduceByKeyAndWindow((v1, v2) => v1 + v2, (v1, v2) => v1 - v2, Seconds(batchDuration * 3),
        Seconds(batchDuration)).print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 9) {
      println("Usage: program progressDir PolicyName PolicyKey EventHubNamespace EventHubName" +
        " BatchDuration(seconds) Spark_Checkpoint_Directory maxRate enqueueTime")
      sys.exit(1)
    }

    val progressDir = args(0)
    val policyName = args(1)
    val policykey = args(2)
    val namespace = args(3)
    val name = args(4)
    val batchDuration = args(5).toInt
    val sparkCheckpointDir = args(6)
    val maxRate = args(7)
    val enqueueTime = args(8)

    val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> policyName,
      "eventhubs.policykey" -> policykey,
      "eventhubs.namespace" -> namespace,
      "eventhubs.name" -> name,
      "eventhubs.partition.count" -> "32",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.maxRate" -> maxRate,
      "eventhubs.filter.enqueuetime" -> enqueueTime
    )

    val ssc = StreamingContext.getOrCreate(sparkCheckpointDir,
      () => createStreamingContext(sparkCheckpointDir, batchDuration, namespace, name,
        eventhubParameters, progressDir))

    ssc.start()
    ssc.awaitTermination()
  }
}
