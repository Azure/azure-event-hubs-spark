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

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.SparkConf
import org.apache.spark.eventhubscommon.client.{
  AMQPEventHubsClient,
  Client,
  EventHubsClientWrapper
}
import org.apache.spark.streaming.StreamingContext

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
   * create direct stream based on eventhubs
   * @param ssc the streaming context this stream belongs to
   * @param eventHubNamespace the namespace of eventhubs
   * @param progressDir the checkpoint directory path (we only support HDFS-based checkpoint
   *                      storage for now, so you have to prefix your path with hdfs://clustername/
   * @param eventParams the parameters of your eventhub instances, format:
   *                    Map[eventhubinstanceName -> Map(parameterName -> parameterValue)
   */
  def createDirectStreams(
      ssc: StreamingContext,
      eventHubNamespace: String,
      progressDir: String,
      eventParams: Map[String, Predef.Map[String, String]]): EventHubDirectDStream = {
    val newStream = new EventHubDirectDStream(ssc,
                                              eventHubNamespace,
                                              progressDir,
                                              eventParams,
                                              EventHubsClientWrapper.apply,
                                              EventHubsClientWrapper.apply)
    newStream
  }

  /**
   * internal API to test, by default, we do not allow user to change eventhubsReceiverCreator and
   * eventHubsClientCreator
   */
  private[eventhubs] def createDirectStreams(
      ssc: StreamingContext,
      eventHubNamespace: String,
      progressDir: String,
      eventParams: Predef.Map[String, Predef.Map[String, String]],
      eventHubsClientCreator: (Map[String, String]) => Client): EventHubDirectDStream = {
    val newStream = new EventHubDirectDStream(ssc,
                                              eventHubNamespace,
                                              progressDir,
                                              eventParams,
                                              EventHubsClientWrapper.apply,
                                              eventHubsClientCreator)
    newStream
  }
}
