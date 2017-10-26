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

package org.apache.spark.eventhubs.common.client

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient }
import org.apache.spark.eventhubs.common.NameAndPartition
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType

private[spark] trait Client extends Serializable {

  private[spark] var client: EventHubClient

  private[spark] def initReceiver(partitionId: String,
                                  offsetType: EventHubsOffsetType,
                                  currentOffset: String): Unit

  def receive(expectedEvents: Int): Iterable[EventData]

  /**
   * Provides the earliest (lowest) sequence number that exists in the EventHubs instance
   * for the given partition.
   *
   * @return a map from eventhubName-partition to seq
   */
  def earliestSeqNo(eventHubNameAndPartition: NameAndPartition): Option[Long]

  /**
   * Provides the last (highest) sequence number and offset that exists in the EventHubs
   * instance for the given partition.
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  def lastOffsetAndSeqNo(eventHubNameAndPartition: NameAndPartition): (Long, Long)

  /**
   * Provides the last (highest) enqueue time of an event for a given partition.
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): Option[Long]

  /**
   * Closes the EventHubs client.
   */
  def close(): Unit
}
