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

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient, PartitionReceiver }
import org.apache.spark.eventhubs.common._
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType

private[spark] trait Client extends Serializable {

  private[spark] var client: EventHubClient

  private[spark] def receiver(partitionId: String, seqNo: SequenceNumber): PartitionReceiver

  def receive(expectedEvents: Int): Iterable[EventData]

  /**
   * Provides the earliest (lowest) sequence number that exists in the EventHubs instance
   * for the given partition.
   *
   * @return a map from eventhubName-partition to seq
   */
  def earliestSeqNo(eventHubNameAndPartition: NameAndPartition): SequenceNumber

  /**
   * Provides the last (highest) sequence number and offset that exists in the EventHubs
   * instance for the given partition.
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  def latestSeqNo(partitionId: PartitionId): SequenceNumber

  /**
   * Provides the last (highest) enqueue time of an event for a given partition.
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): EnqueueTime

  /**
   * Translates any starting point provided by a user to the specific offset and sequence number
   * that we need to start from. This ensure that within Spark, we're only dealing with offsets
   * and sequence numbers.
   */
  def translate[T](ehConf: EventHubsConf): Map[PartitionId, SequenceNumber]

  def partitionCount(): Int

  /**
   * Closes the EventHubs client.
   */
  def close(): Unit
}
