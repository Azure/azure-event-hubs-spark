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
   * Creates a sender which sends directly to the specified partitionId.
   *
   * @param partitionId the partition that will receive all events sent from this partition sender.
   */
  def createPartitionSender(partitionId: Int)

  /**
   * Sends an [[EventData]] to your Event Hub. If a partition is provided, the event
   * will be sent directly to that partition. If a partition key is provided, it will be
   * used to determine the target partition.
   *
   * @param event the event that is being sent.
   * @param partition the partition that will receive all events being sent.
   * @param partitionKey the partitionKey will be hash'ed to determine the partitionId
   *                     to send the events to. On the Received message this can be accessed
   *                     at [[EventData.SystemProperties#getPartitionKey()]]
   */
  def send(event: EventData,
           partition: Option[Int] = None,
           partitionKey: Option[String] = None): Unit

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
   * Provides the earliest and the latest sequence numbers in the provided partition.
   *
   * @return the earliest and latest sequence numbers for the specified partition.
   */
  def boundedSeqNos(partitionId: PartitionId): (SequenceNumber, SequenceNumber)

  /**
   * Translates any starting point provided by a user to the specific offset and sequence number
   * that we need to start from. This ensure that within Spark, we're only dealing with offsets
   * and sequence numbers.
   */
  def translate[T](ehConf: EventHubsConf,
                   partitionCount: Int,
                   useStart: Boolean = true): Map[PartitionId, SequenceNumber]

  /**
   * Returns the number of partitions in your EventHubs instance.
   */
  def partitionCount: Int

  /**
   * Closes the EventHubs client.
   */
  def close(): Unit
}
