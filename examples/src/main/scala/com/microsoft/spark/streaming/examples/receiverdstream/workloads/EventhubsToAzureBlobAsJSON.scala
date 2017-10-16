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

package com.microsoft.spark.streaming.examples.receiverdstream.workloads

import com.microsoft.spark.streaming.examples.receiverdstream.arguments.{EventhubsArgumentKeys, EventhubsArgumentParser}
import com.microsoft.spark.streaming.examples.receiverdstream.arguments.EventhubsArgumentParser.ArgumentMap
import com.microsoft.spark.streaming.examples.receiverdstream.common.{EventContent, StreamStatistics}

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

object EventhubsToAzureBlobAsJSON {

  def createStreamingContext(inputOptions: ArgumentMap): StreamingContext = {

    val eventHubsParameters = Map[String, String](
      "eventhubs.namespace" -> inputOptions(Symbol(EventhubsArgumentKeys.EventhubsNamespace)).asInstanceOf[String],
      "eventhubs.name" -> inputOptions(Symbol(EventhubsArgumentKeys.EventhubsName)).asInstanceOf[String],
      "eventhubs.policyname" -> inputOptions(Symbol(EventhubsArgumentKeys.PolicyName)).asInstanceOf[String],
      "eventhubs.policykey" -> inputOptions(Symbol(EventhubsArgumentKeys.PolicyKey)).asInstanceOf[String],
      "eventhubs.consumergroup" -> inputOptions(Symbol(EventhubsArgumentKeys.ConsumerGroup)).asInstanceOf[String],
      "eventhubs.partition.count" -> inputOptions(Symbol(EventhubsArgumentKeys.PartitionCount))
      .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.interval" -> inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
      .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.dir" -> inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String]
    )

    /**
     * In Spark 2.0.x, SparkConf must be initialized through EventhubsUtil so that required
     * data structures internal to Azure Eventhubs Client get registered with the Kryo Serializer.
     */
    val sparkConfiguration : SparkConf = EventHubsUtils.initializeSparkStreamingConfigurations

    sparkConfiguration.setAppName(this.getClass.getSimpleName)
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.allowBatching", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.batchingTimeout", "60000")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkSession : SparkSession = SparkSession.builder.config(sparkConfiguration).getOrCreate

    val streamingContext = new StreamingContext(sparkSession.sparkContext,
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))
    streamingContext.checkpoint(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).
      asInstanceOf[String])

    val eventHubsStream = EventHubsUtils.createUnionStream(streamingContext, eventHubsParameters)

    val eventHubsWindowedStream = eventHubsStream.window(
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))

    eventHubsWindowedStream.map(x => EventContent(new String(x)))
      .foreachRDD(rdd => {
        val sparkSession = SparkSession.builder.getOrCreate
        import sparkSession.implicits._
        rdd.toDS.toJSON.write.mode(SaveMode.Overwrite)
          .save(inputOptions(Symbol(EventhubsArgumentKeys.EventStoreFolder)).asInstanceOf[String])
      })

    // Count number of events received the past batch

    val batchEventCount = eventHubsWindowedStream.count()

    batchEventCount.print()

    // Count number of events received so far

    val totalEventCountDStream =
      eventHubsWindowedStream.map(m => (StreamStatistics.streamLengthKey, 1L))
    val totalEventCount =
      totalEventCountDStream.updateStateByKey[Long](StreamStatistics.streamLength)
    totalEventCount.checkpoint(
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))

    if (inputOptions.contains(Symbol(EventhubsArgumentKeys.EventCountFolder))) {

      totalEventCount.saveAsTextFiles(inputOptions(Symbol(EventhubsArgumentKeys.EventCountFolder))
        .asInstanceOf[String])
    }

    totalEventCount.print()

    streamingContext
  }

  def main(inputArguments: Array[String]): Unit = {

    val inputOptions = EventhubsArgumentParser.parseArguments(Map(), inputArguments.toList)

    EventhubsArgumentParser.verifyEventhubsToAzureBlobAsJSONArguments(inputOptions)

    val streamingContext = StreamingContext.getOrCreate(
      inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String],
        () => createStreamingContext(inputOptions))

    streamingContext.start()

    if(inputOptions.contains(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))) {
      streamingContext.awaitTerminationOrTimeout(
        inputOptions(Symbol(EventhubsArgumentKeys.TimeoutInMinutes)).asInstanceOf[Long] * 60 * 1000)
    }
    else {

      streamingContext.awaitTermination()
    }
  }
}

