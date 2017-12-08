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

package org.apache.spark.eventhubs

import java.time.Duration

import com.microsoft.azure.eventhubs.{ EventHubClient, PartitionReceiver }

package object common {
  val DefaultEnqueueTime: EnqueueTime = Long.MinValue
  val StartOfStream = PartitionReceiver.START_OF_STREAM
  val EndOfStream = PartitionReceiver.END_OF_STREAM
  val DefaultMaxRatePerPartition: Rate = 10000
  val DefaultStartOffset: Offset = -1L
  val DefaultStartSequenceNumber: SequenceNumber = 0L
  val DefaultReceiverTimeout: Duration = Duration.ofSeconds(5)
  val DefaultOperationTimeout: Duration = Duration.ofSeconds(60)
  val DefaultConsumerGroup: String = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME
  val PrefetchCountMinimum
    : Int = 10 // Change this to PartitionReceiver.PREFETCH_COUNT_MINIMUM on next client release.

  type PartitionId = Int
  type Rate = Int
  type Offset = Long
  type EnqueueTime = Long
  type SequenceNumber = Long

  // Allow Strings to be converted to types defined in this library.
  implicit class EventHubsString(val str: String) extends AnyVal {
    def toPartitionId: PartitionId = str.toInt
    def toRate: Rate = str.toInt
    def toOffset: Offset = str.toLong
    def toEnqueueTime: EnqueueTime = str.toLong
    def toSequenceNumber: SequenceNumber = str.toLong
  }

  // TODO: just so this builds. Remove this once API is actually available from EH Java Client.
  implicit class SeqNoAPI(val client: EventHubClient) extends AnyVal {
    def createReceiver(consumerGroup: String,
                       partitionId: String,
                       seqNo: Long,
                       inclusiveSeqNo: Boolean): PartitionReceiver =
      null
  }
}
