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

package org.apache.spark.eventhubs.client

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs._

private[spark] trait Client extends Serializable {

  /**
   * Creates an EventHub receiver.
   *
   * @param partitionId the partitionId the receiver will consume from.
   * @param startingSeqNo the sequence number the receiver will start from.
   */
  def createReceiver(partitionId: String, startingSeqNo: SequenceNumber): Unit

  /**
   * Receive events from your EventHubs instance.
   *
   * @param eventCount the number of events that will be requested from the EventHub partition.
   */
  def receive(eventCount: Int): java.lang.Iterable[EventData]

  /**
   * When a connection with the service is established, the client will begin to prefetch EventData.
   * This number specifies the max number of events that will prefetched.
   */
  def setPrefetchCount(count: Int): Unit

  /**
   * Provides the earliest (lowest) sequence number that exists in the EventHubs instance
   * for the given partition.
   *
   * @return the earliest sequence number for the specified partition
   */
  def earliestSeqNo(partitionId: PartitionId): SequenceNumber

  /**
   * Provides the latest (highest) sequence number that exists in the EventHubs
   * instance for the given partition.
   *
   * @return the leatest sequence number for the specified partition
   */
  def latestSeqNo(partitionId: PartitionId): SequenceNumber

  /**
   * Translates any starting point provided by a user to the specific offset and sequence number
   * that we need to start from. This ensure that within Spark, we're only dealing with offsets
   * and sequence numbers.
   */
  def translate[T](ehConf: EventHubsConf,
                   partitionCount: Int,
                   useStart: Boolean = false): Map[PartitionId, SequenceNumber]

  /**
   * Returns the number of partitions in your EventHubs instance.
   */
  def partitionCount: Int

  /**
   * Closes the EventHubs client.
   */
  def close(): Unit
}
