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

package org.apache.spark.eventhubs.common.utils

import org.apache.spark.internal.Logging

/**
 */
private[spark] class EventHubsTestUtils extends Logging {

  /*
  Create a MockEventHub
  Populate it easily

  MockClient:
      MockEventHubs:
    - Partition count
    - MockPartitions
      - MockEventData
        - enqueue time, sequence number, offset, and body
      - size
    - def receive(expectedEvents: Int): Iterable[EventData]
    - def earliestSeqNo(eventHubNameAndPartition: NameAndPartition): SequenceNumber
    - def latestSeqNo(partitionId: PartitionId): SequenceNumber
    - def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): EnqueueTime
    - def translate[T](ehConf: EventHubsConf): Map[PartitionId, SequenceNumber]
    - def partitionCount(): Int
    - def close(): Unit
 */
}
