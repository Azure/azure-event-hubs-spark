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

object MultiStreamsJoin {

  private def createNewStreamingContext(
      sparkCheckpointDir: String,
      progressDir: String,
      policyNames: String,
      policyKeys: String,
      namespaces: String,
      names: String,
      batchDuration: Int,
      rate: Int): StreamingContext = {

    val ssc = new StreamingContext(new SparkContext(), Seconds(batchDuration))
    ssc.checkpoint(sparkCheckpointDir)

    val Array(policyName1, policyName2) = policyNames.split(",")
    val Array(policykey1, policykey2) = policyKeys.split(",")
    val Array(namespace1, namespace2) = namespaces.split(",")
    val Array(name1, name2) = names.split(",")

    val eventhubParameters = (name: String,
                              namespace: String,
                              policyName: String,
                              policyKey: String) => Map(name -> Map[String, String] (
      "eventhubs.policyname" -> policyName,
      "eventhubs.policykey" -> policyKey,
      "eventhubs.namespace" -> namespace,
      "eventhubs.name" -> name,
      "eventhubs.partition.count" -> "32",
      "eventhubs.maxRate" -> s"$rate",
      "eventhubs.consumergroup" -> "$Default"
    ))

    val inputDirectStream1 = EventHubsUtils.createDirectStreams(
      ssc,
      namespace1,
      progressDir,
      eventhubParameters(name1, namespace1, policyName1, policykey1))

    val inputDirectStream2 = EventHubsUtils.createDirectStreams(
      ssc,
      namespace2,
      progressDir,
      eventhubParameters(name2, namespace2, policyName2, policykey2))

    val kv1 = inputDirectStream1.map(receivedRecord => (new String(receivedRecord.getBody), 1))
      .reduceByKey(_ + _)
    val kv2 = inputDirectStream2.map(receivedRecord => (new String(receivedRecord.getBody), 1))
      .reduceByKey(_ + _)

    kv1.join(kv2).map {
      case (k, (count1, count2)) =>
        (k, count1 + count2)
    }.print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 8) {
      println("Usage: program progressDir PolicyName1,PolicyName2 PolicyKey1,PolicyKey2" +
        " EventHubNamespace1,EventHubNamespace2 EventHubName1,EventHubName2" +
        " BatchDuration(seconds)")
      sys.exit(1)
    }

    val progressDir = args(0)
    val policyNames = args(1)
    val policyKeys = args(2)
    val namespaces = args(3)
    val names = args(4)
    val batchDuration = args(5).toInt
    val sparkCheckpointDir = args(6)
    val rate = args(7).toInt

    val ssc = StreamingContext.getOrCreate(sparkCheckpointDir, () =>
      createNewStreamingContext(sparkCheckpointDir, progressDir, policyNames, policyKeys,
        namespaces, names, batchDuration, rate))
    ssc.start()
    ssc.awaitTermination()
  }
}