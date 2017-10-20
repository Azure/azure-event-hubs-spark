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

package org.apache.spark.eventhubscommon.client

import com.microsoft.azure.eventhubs.{ EventData, EventHubClient }
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType

private[spark] trait Client extends Serializable {

  private[spark] var client: EventHubClient = _

  // TODO can we get rid of EventData with type parameters?
  def receive(expectedEvents: Int): Iterable[EventData]

  private[spark] def initClient(): Unit

  private[spark] def initReceiver(partitionId: String,
                                  offsetType: EventHubsOffsetType,
                                  currentOffset: String): Unit

  /**
   * return the start seq number of each partition
   * @return a map from eventhubName-partition to seq
   */
  def startSeqOfPartition(retryIfFail: Boolean,
                          targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List())
    : Option[Map[EventHubNameAndPartition, Long]]

  /**
   * return the end point of each partition
   * @return a map from eventhubName-partition to (offset, seq)
   */
  def endPointOfPartition(retryIfFail: Boolean,
                          targetEventHubNameAndPartitions: List[EventHubNameAndPartition] = List())
    : Option[Map[EventHubNameAndPartition, (Long, Long)]]

  /**
   * return the last enqueueTime of each partition
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  def lastEnqueueTimeOfPartitions(retryIfFail: Boolean,
                                  targetEventHubNameAndPartitions: List[EventHubNameAndPartition])
    : Option[Map[EventHubNameAndPartition, Long]]

  /**
   * close this client
   */
  def close(): Unit
}
