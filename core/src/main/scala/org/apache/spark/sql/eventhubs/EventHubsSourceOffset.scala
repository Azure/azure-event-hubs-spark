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

package org.apache.spark.sql.eventhubs

import org.apache.spark.eventhubs.{ NameAndPartition, SequenceNumber }
import org.apache.spark.sql.execution.streaming.{ Offset, SerializedOffset }

/**
 * An [[Offset]] for the [[EventHubsSource]]. This tracks all partitions and their offsets.
 */
private[eventhubs] case class EventHubsSourceOffset(
    partitionToSeqNos: Map[NameAndPartition, SequenceNumber])
    extends Offset {

  override val json: String = JsonUtils.partitionSeqNos(partitionToSeqNos)
}

/**
 * A companion object of the [[EventHubsSourceOffset]].
 */
private[eventhubs] object EventHubsSourceOffset {

  def getPartitionSeqNos(offset: Offset): Map[NameAndPartition, SequenceNumber] = {
    offset match {
      case o: EventHubsSourceOffset => o.partitionToSeqNos
      case so: SerializedOffset     => EventHubsSourceOffset(so).partitionToSeqNos
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to EventHubsSourceOffset")
    }
  }

  /**
   * Creates an [[EventHubsSourceOffset]] from a variable sequence of
   * (Event Hub name, partitionId, sequence number) tuples.
   */
  def apply(offsetTuples: (String, Int, SequenceNumber)*): EventHubsSourceOffset = {
    EventHubsSourceOffset(
      offsetTuples.map { case (n, p, s) => (new NameAndPartition(n, p), s) }.toMap)
  }

  /**
   * Creates an [[EventHubsSourceOffset]] from a JSON [[SerializedOffset]].
   */
  def apply(offset: SerializedOffset): EventHubsSourceOffset = {
    EventHubsSourceOffset(JsonUtils.partitionSeqNos(offset.json))
  }
}
