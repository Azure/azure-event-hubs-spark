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

object Test {

  def main(args: Array[String]): Unit = {
    val client = RestfulEventHubClient.getInstance(
      "nanzhu-hdinsight-eastasia-2",
      Map("hub1_multi_join1" -> Map("eventhubs.policyname" -> "policy1",
        "eventhubs.policykey" -> "WxCFSgyAa+Sjle2ulQVSgjen3rKcaWVtYJTUtctWb/0=",
        "eventhubs.namespace" -> "nanzhu-hdinsight-eastasia-2",
        "eventhubs.name" -> "hub1_multijoin1",
        "eventhubs.partition.count" -> "32",
        "eventhubs.consumergroup" -> "$Default")))

    println(client.endPointOfPartition())
  }
}
