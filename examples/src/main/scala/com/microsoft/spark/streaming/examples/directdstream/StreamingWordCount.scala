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
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.eventhubs.common.{ EventHubsConf, EventHubsUtils }

/**
 * an example application of Streaming WordCount
 */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      println(
        "Usage: program progressDir PolicyName PolicyKey EventHubNamespace EventHubName" +
          " BatchDuration(seconds)")
      sys.exit(1)
    }

    val progressDir = args(0)
    val keyName = args(1)
    val key = args(2)
    val namespace = args(3)
    val name = args(4)
    val batchDuration = args(5).toInt

    val ehConf = EventHubsConf()
      .setNamespace(namespace)
      .setName(name)
      .setKeyName(keyName)
      .setKey(key)
      .setProgressDirectory(progressDir)
      .setPartitionCount("32")
      .setConsumerGroup("$Default")

    val ssc = new StreamingContext(new SparkContext(), Seconds(batchDuration))

    val inputDirectStream = EventHubsUtils.createDirectStream(ssc, ehConf)

    inputDirectStream.foreachRDD { rdd =>
      rdd
        .flatMap(eventData =>
          new String(eventData.getBody).split(" ").map(_.replaceAll("[^A-Za-z0-9 ]", "")))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .collect()
        .toList
        .foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
