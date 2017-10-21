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

package org.apache.spark.eventhubscommon.utils

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient }
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.Client
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType

/**
 * TestClientSugar implements Client so all methods and variables are NOPs. This will
 * reduce repetitive code when the Client is mocked in testing.
 */
trait TestClientSugar extends Client {
  override private[spark] var client: EventHubClient = _

  override def close(): Unit = {}

  override def lastSeqAndOffset(
      eventHubNameAndPartition: EventHubNameAndPartition): Option[(Long, Long)] = Option.empty

  override private[spark] def initReceiver(partitionId: String,
                                           offsetType: EventHubsOffsetType,
                                           currentOffset: String) = {}

  override def lastEnqueuedTime(eventHubNameAndPartition: EventHubNameAndPartition): Option[Long] =
    Option.empty

  override def receive(expectedEvents: Int): Iterable[EventData] = Iterable[EventData]()

  override def beginSeqNo(eventHubNameAndPartition: EventHubNameAndPartition): Option[Long] =
    Option.empty
}
