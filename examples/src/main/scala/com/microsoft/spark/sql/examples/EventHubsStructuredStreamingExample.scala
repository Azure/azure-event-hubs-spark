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

package com.microsoft.spark.sql.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime

object EventHubsStructuredStreamingExample {

  def main(args: Array[String]): Unit = {

    if (args.length != 8) {
      println("Usage: program progressDir PolicyName PolicyKey EventHubNamespace EventHubName" +
        " maxRate checkpointLocation outputPath")
      sys.exit(1)
    }

    val progressDir = args(0)
    val policyName = args(1)
    val policyKey = args(2)
    val eventHubNamespace = args(3)
    val eventHubName = args(4)
    val maxRate = args(5).toInt
    val checkpointLocation = args(6)
    val outputPath = args(7)

    val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> policyName,
      "eventhubs.policykey" -> policyKey,
      "eventhubs.namespace" -> eventHubNamespace,
      "eventhubs.name" -> eventHubName,
      "eventhubs.partition.count" -> "32",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> progressDir,
      "eventhubs.maxRate" -> s"$maxRate",
      "eventhubs.sql.containsProperties" -> "true",
      "eventhubs.sql.userDefinedKeys" -> "creationTime,randomUserProperty"
    )

    val sparkSession = SparkSession.builder().getOrCreate()
    val inputStream = sparkSession.readStream.format("eventhubs").options(eventhubParameters)
      .load()
    val streamingQuery1 = inputStream.writeStream.
      outputMode("append").
      trigger(ProcessingTime("10 seconds")).
      option("checkpointLocation", checkpointLocation).
      format("parquet").option("path", outputPath + "/ETL").partitionBy("creationTime").start()
    streamingQuery1.awaitTermination()
  }
}
