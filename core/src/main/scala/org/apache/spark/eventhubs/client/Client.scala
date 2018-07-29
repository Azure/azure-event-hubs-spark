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

import com.microsoft.azure.eventhubs.{ EventData, PartitionSender }
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs._

/**
 * In order to deliver Event Hubs data in Spark, we must collect various
 * data and metadata from the Event Hubs service. [[Client]] contains
 * all client operations done between Spark and the Event Hubs service.
 */
private[spark] trait Client extends Serializable {

  /**
   * Creates a [[PartitionSender]] which sends directly to the specified
   * partition.
   *
   * @param partition the partition that will receive all events sent
   *                    from this partition sender.
   */
  def createPartitionSender(partition: Int)

  /**
   * Sends an [[EventData]] to your Event Hub. If a partition is provided,
   * the event will be sent directly to that partition. If a partition
   * key is provided, it will be used to determine the target partition.
   *
   * @param event the event that is being sent.
   * @param partition the partition that will receive all events being sent.
   * @param partitionKey the partitionKey will be hash'ed to determine the
   *                     partition to send the events to. On the Received
   *                     message this can be accessed at
   *                     [[EventData.SystemProperties#getPartitionKey()]]
   */
  def send(event: EventData,
           partition: Option[Int] = None,
           partitionKey: Option[String] = None): Unit

  /**
   * Returns the earliest and latest sequence numbers for all partitions
   * in an Event Hub.
   *
   * @return the earliest and latest sequence numbers for all partitions in the Event Hub
   */
  def allBoundedSeqNos: Map[PartitionId, (SequenceNumber, SequenceNumber)]

  /**
   * Translates all [[EventPosition]]s provided in the [[EventHubsConf]] to
   * sequence numbers. Sequence numbers are zero-based indices. The 5th event
   * in an Event Hubs partition will have a sequence number of 4.
   *
   * This allows us to exclusively use sequence numbers to generate and manage
   * batches within Spark (rather than coding for many different filter types).
   *
   * @param ehConf the [[EventHubsConf]] containing starting (or ending positions)
   * @param partitionCount the number of partitions in the Event Hub instance
   * @param useStart translates starting positions when true and ending positions
   *                 when false
   * @return mapping of partitions to starting positions as sequence numbers
   */
  def translate(ehConf: EventHubsConf,
                partitionCount: Int,
                useStart: Boolean = true): Map[PartitionId, SequenceNumber]

  /**
   * Returns the number of partitions in your EventHubs instance.
   */
  def partitionCount: Int

  /**
   * Cleans up all open connections and AMQP links.
   */
  def close(): Unit
}
