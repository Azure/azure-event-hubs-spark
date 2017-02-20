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

package org.apache.spark.sql.streaming.eventhubs

import org.apache.spark.eventhubscommon.{CommonUtils, EventHubNameAndPartition}
import org.apache.spark.eventhubscommon.client.{EventHubClient, EventHubsClientWrapper, RestfulEventHubClient}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types._

/**
 * each source is mapped to an eventhubs instance
 */
private[eventhubs] class EventHubsSource(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    eventhubReceiverCreator: (Map[String, String], Int, Long, Int) => EventHubsClientWrapper =
      EventHubsClientWrapper.getEventHubReceiver,
    eventhubClientCreator: (String, Map[String, Map[String, String]]) => EventHubClient =
      RestfulEventHubClient.getInstance) extends Source with Logging {

  private val eventhubsNamespace: String = parameters("eventhubs.namespace")

  require(eventhubsNamespace != null, "eventhubs.namespace is not defined")

  private val eventhubsName: String = parameters("eventhubs.name")

  require(eventhubsName != null, "eventhubs.name is not defined")

  private var _eventHubClient: EventHubClient = _

  private[eventhubs] def eventHubClient = {
    if (_eventHubClient == null) {
      _eventHubClient = eventhubClientCreator(eventhubsNamespace, Map(eventhubsName -> parameters))
    }
    _eventHubClient
  }

  private[eventhubs] def setEventHubClient(eventHubClient: EventHubClient): EventHubsSource = {
    _eventHubClient = eventHubClient
    this
  }

  private val ehNameAndPartitions = {
    val partitionCount = parameters("eventhubs.partition.count").toInt
    for (partitionId <- 0 until partitionCount)
      yield EventHubNameAndPartition(eventhubsName, partitionId)
  }

  private var currentOffsetsAndSeqNums: EventHubsOffset =
    EventHubsOffset(ehNameAndPartitions.map{ehNameAndSpace => (ehNameAndSpace, (-1L, -1L))}.toMap)
  private var fetchedHighestOffsetsAndSeqNums: EventHubsOffset = _

  override def schema: StructType = {
    val userDefinedKeys = parameters.get("eventhubs.sql.userDefinedKeys") match {
      case Some(keys) =>
        keys.split(",").toSeq
      case None =>
        Seq()
    }
    EventHubsSourceProvider.sourceSchema(userDefinedKeys)
  }

  private[spark] def composeHighestOffset(retryIfFail: Boolean) = {
    CommonUtils.fetchLatestOffset(eventHubClient, retryIfFail = retryIfFail) match {
      case Some(highestOffsets) =>
        fetchedHighestOffsetsAndSeqNums = EventHubsOffset(highestOffsets)
        Some(fetchedHighestOffsetsAndSeqNums.offsets)
      case _ =>
        logWarning(s"failed to fetch highest offset")
        if (retryIfFail) {
          None
        } else {
          Some(fetchedHighestOffsetsAndSeqNums.offsets)
        }
    }
  }

  /**
   * when we have reached the end of the message queue in the remote end or we haven't get any
   * idea about the highest offset, we shall fail the app when rest endpoint is not responsive, and
   * to prevent we die too much, we shall retry with 2-power interval in this case
   */
  private def failAppIfRestEndpointFail = fetchedHighestOffsetsAndSeqNums == null ||
    currentOffsetsAndSeqNums.offsets.equals(fetchedHighestOffsetsAndSeqNums.offsets)

  /**
   * @return return the target offset in the next batch
   */
  override def getOffset: Option[Offset] = {
    // 1. get the highest offset
    val highestOffsets = composeHighestOffset(failAppIfRestEndpointFail)
    // 2. rate limit
    null
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    null
  }

  override def stop(): Unit = {

  }
}
