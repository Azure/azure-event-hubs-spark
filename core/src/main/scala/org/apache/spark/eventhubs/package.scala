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

package org.apache.spark

import java.time.Duration
import java.util.concurrent.{ ExecutorService, Executors }

import com.microsoft.azure.eventhubs.{ EventHubClient, PartitionReceiver }
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

package object eventhubs {

  implicit val formats = Serialization.formats(NoTypeHints)

  val DefaultEventPosition: EventPosition = EventPosition.fromStartOfStream
  val DefaultEndingPosition: EventPosition = EventPosition.fromEndOfStream
  val DefaultMaxRatePerPartition: Rate = 1000
  val DefaultReceiverTimeout: Duration = Duration.ofSeconds(60)
  val DefaultOperationTimeout: Duration = Duration.ofSeconds(60)
  val DefaultConsumerGroup: String = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME
  val PrefetchCountMinimum: Int = PartitionReceiver.MINIMUM_PREFETCH_COUNT
  val DefaultFailOnDataLoss = "true"
  val DefaultUseSimulatedClient = "false"
  val StartingSequenceNumber = 0L

  type PartitionId = Int
  val PartitionId = Int

  type Rate = Int
  val Rate = Int

  type Offset = Long
  val Offset = Long

  type EnqueueTime = Long
  val EnqueueTime = Long

  type SequenceNumber = Long
  val SequenceNumber = Long

  // Allow Strings to be converted to types defined in this library.
  implicit class EventHubsString(val str: String) extends AnyVal {
    def toPartitionId: PartitionId = str.toInt
    def toRate: Rate = str.toInt
    def toOffset: Offset = str.toLong
    def toEnqueueTime: EnqueueTime = str.toLong
    def toSequenceNumber: SequenceNumber = str.toLong
  }
}
