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

import com.microsoft.azure.eventhubs.{ EventHubClient, PartitionReceiver }
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * A package object to constants, implicit conversion, and type
 * aliases used throughout the connector.
 */
package object eventhubs {

  implicit val formats = Serialization.formats(NoTypeHints)

  val StartOfStream: String = "-1"
  val EndOfStream: String = "@latest"
  val DefaultEventPosition: EventPosition = EventPosition.fromEndOfStream
  val DefaultEndingPosition: EventPosition = EventPosition.fromEndOfStream
  val DefaultMaxRatePerPartition: Rate = 1000
  val DefaultReceiverTimeout: Duration = Duration.ofSeconds(60)
  val DefaultOperationTimeout: Duration = Duration.ofSeconds(300)
  val DefaultMaxBatchReceiveTime: Duration = Duration.ofSeconds(5)
  val DefaultConsumerGroup: String = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME
  val PrefetchCountMinimum: Int = PartitionReceiver.MINIMUM_PREFETCH_COUNT
  val PrefetchCountMaximum: Int = PartitionReceiver.MAXIMUM_PREFETCH_COUNT
  val DefaultPrefetchCount: Int = PartitionReceiver.DEFAULT_PREFETCH_COUNT
  val DefaultFailOnDataLoss = "true"
  val DefaultUseSimulatedClient = "false"
  val DefaultPartitionPreferredLocationStrategy = "Hash"
  val DefaultUseExclusiveReceiver = "true"
  val StartingSequenceNumber = 0L
  val DefaultThreadPoolSize = 16
  val DefaultEpoch = 0L
  val RetryCount = 10
  val WaitInterval = 5000

  val OffsetAnnotation = "x-opt-offset"
  val EnqueuedTimeAnnotation = "x-opt-enqueued-time"
  val SequenceNumberAnnotation = "x-opt-sequence-number"

  val SparkConnectorVersion = "2.3.15"

  type PartitionId = Int
  val PartitionId: Int.type = Int

  type Rate = Int
  val Rate: Int.type = Int

  type Offset = Long
  val Offset: Long.type = Long

  type EnqueueTime = Long
  val EnqueueTime: Long.type = Long

  type SequenceNumber = Long
  val SequenceNumber: Long.type = Long

  object PartitionPreferredLocationStrategy extends Enumeration {
    type PartitionPreferredLocationStrategy = Value
    val Hash, BalancedHash = Value
  }

  // Allow Strings to be converted to types defined in this library.
  implicit class EventHubsString(val str: String) extends AnyVal {
    def toPartitionId: PartitionId = str.toInt

    def toRate: Rate = str.toInt

    def toOffset: Offset = str.toLong

    def toEnqueueTime: EnqueueTime = str.toLong

    def toSequenceNumber: SequenceNumber = str.toLong
  }
}
