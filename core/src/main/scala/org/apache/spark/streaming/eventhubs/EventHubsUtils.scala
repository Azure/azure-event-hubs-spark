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

import scala.collection.Map

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.SparkConf
import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper}
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.eventhubs.checkpoint.OffsetStore
import org.apache.spark.streaming.receiver.Receiver


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

  // scalastyle:off
  /**
   * Create a unioned EventHubs stream that receives data from Microsoft Azure Eventhubs
   * The unioned stream will receive message from all partitions of the EventHubs
   *
   * @param streamingContext Streaming Context object
   * @param eventhubsParams  a Map that contains parameters for EventHubs.
   *                         Required parameters are:
   *                         "eventhubs.policyname": EventHubs policy name
   *                         "eventhubs.policykey": EventHubs policy key
   *                         "eventhubs.namespace": EventHubs namespace
   *                         "eventhubs.name": EventHubs name
   *                         "eventhubs.partition.count": Number of partitions
   *                         "eventhubs.checkpoint.dir": checkpoint directory on HDFS
   *
   *                         Optional parameters are:
   *                         "eventhubs.consumergroup": EventHubs consumer group name, default to "\$default"
   *                         "eventhubs.filter.offset": Starting offset of EventHubs, default to "-1"
   *                         "eventhubs.filter.enqueuetime": Unix time, seconds since epoch, default to "0"
   *                         "eventhubs.default.credits": default AMQP credits, default to -1 (which is 1024)
   *                         "eventhubs.checkpoint.interval": checkpoint interval in second, default to 10
   * @param storageLevel     Storage level, by default it is MEMORY_ONLY
   * @return ReceiverInputStream
   */
  // scalastyle:on
  @deprecated("this method is deprecated, please use createDirectStreams", "2.0.5")
  def createUnionStream(streamingContext: StreamingContext, eventhubsParams: Map[String, String],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER): DStream[Array[Byte]] = {
    val partitionCount = eventhubsParams("eventhubs.partition.count").toInt
    val streams = (0 until partitionCount).map {
      i => createStream(streamingContext, eventhubsParams, i.toString, storageLevel)
    }
    streamingContext.union(streams)
  }

  /**
   * Create a single EventHubs stream that receives data from Microsoft Azure EventHubs
   * A single stream only receives message from one EventHubs partition
   *
   * @param streamingContext Streaming Context object
   * @param eventhubsParams  a Map that contains parameters for EventHubs. Same as above.
   * @param partitionId      Partition ID
   * @param storageLevel     Storage level
   * @param offsetStore      Offset store implementation, defaults to DFSBasedOffsetStore
   * @param receiverClient   the EventHubs client implementation, defaults to EventHubsClientWrapper
   * @return ReceiverInputStream
   */
  @deprecated("this method is deprecated, please use createDirectStreams", "2.0.5")
  def createStream(streamingContext: StreamingContext,
                   eventhubsParams: Map[String, String],
                   partitionId: String,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
                   offsetStore: OffsetStore = null,
                   receiverClient: EventHubsClientWrapper = new EventHubsClientWrapper):
      ReceiverInputDStream[Array[Byte]] = {
    streamingContext.receiverStream(
      getReceiver(streamingContext, eventhubsParams.toMap, partitionId,
      storageLevel, Option(offsetStore), receiverClient))
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
      eventParams: Predef.Map[String, Predef.Map[String, String]]): EventHubDirectDStream = {
    val newStream = new EventHubDirectDStream(ssc, eventHubNamespace, progressDir, eventParams)
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
      eventHubsReceiverCreator: (Predef.Map[String, String], Int, Long, EventHubsOffsetType, Int) =>
        EventHubsClientWrapper = EventHubsClientWrapper.getEventHubReceiver,
      eventHubsClientCreator: (String, Predef.Map[String, Predef.Map[String, String]]) =>
        EventHubClient): EventHubDirectDStream = {
    val newStream = new EventHubDirectDStream(ssc, eventHubNamespace, progressDir, eventParams,
      eventHubsReceiverCreator, eventHubsClientCreator)
    newStream
  }

  /**
   * A helper function to get EventHubsReceiver or ReliableEventHubsReceiver based on whether
   * Write Ahead Log is enabled or not ("spark.streaming.receiver.writeAheadLog.enable")
   */
  private[eventhubs] def getReceiver(streamingContext: StreamingContext,
      eventhubsParams: scala.collection.immutable.Map[String, String],
      partitionId: String,
      storageLevel: StorageLevel,
      offsetStore: Option[OffsetStore],
      receiverClient: EventHubsClientWrapper): Receiver[Array[Byte]] = {
    val maximumEventRate = streamingContext.conf.getInt("spark.streaming.receiver.maxRate", 0)
    val walEnabled = streamingContext.conf.getBoolean(
      "spark.streaming.receiver.writeAheadLog.enable", false)

    if (walEnabled) {
      new ReliableEventHubsReceiver(eventhubsParams, partitionId, storageLevel, offsetStore,
        receiverClient, maximumEventRate)
    } else {
      new EventHubsReceiver(eventhubsParams, partitionId, storageLevel, offsetStore, receiverClient,
        maximumEventRate)
    }
  }
}
