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

package org.apache.spark.streaming.eventhubs

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

// TODO: delete this file right before merge
object Test {
  def main(args: Array[String]): Unit = {
    val checkpointDir = args(0)
    val policyName = args(1)
    val policykey = args(2)
    val namespace = args(3)
    val name = args(4)

    val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> policyName,
      "eventhubs.policykey" -> policykey,
      "eventhubs.namespace" -> namespace,
      "eventhubs.name" -> name,
      "eventhubs.partition.count" -> "32",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.maxRate" -> "1000"
    )

    val ssc = StreamingContext.getOrCreate("hdfs://mycluster/checkpoint_spark",
      () => new StreamingContext(new SparkContext(), Seconds(10)))

    val inputDirectStream = new EventHubDirectDStream(ssc, namespace,
      checkpointDir, Map(name -> eventhubParameters))
    var l = 0L
    inputDirectStream.foreachRDD { rdd =>
      val r = rdd.count()
      l += r
      println("current long " + r)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
