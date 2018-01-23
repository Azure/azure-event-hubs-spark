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

import com.microsoft.azure.eventhubs.EventPosition
import java.time.Instant

import org.apache.spark.eventhubs.common.{ EventHubsConf, SequenceNumber }
import org.apache.spark.eventhubs.common.utils.Position.FilterType.FilterType

/**
 * Defines a position of an event in an event hub partition.
 * The position can be an Offset, Sequence Number, or EnqueuedTime.
 *
 * This event is passed to the EventHubsConf to define a starting point for your Spark job.
 */
class Position extends Serializable {

  import Position._

  private def this(o: String, i: Boolean) {
    this
    offset = o
    isInclusive = i
    filterType = FilterType.Offset
  }

  private def this(s: SequenceNumber, i: Boolean) {
    this
    seqNo = s
    isInclusive = i
    filterType = FilterType.SequenceNumber
  }

  private def this(e: Instant) {
    this
    enqueuedTime = e
    filterType = FilterType.EnqueuedTime
  }

  private var filterType: FilterType = _
  private var offset: String = _
  private[common] var seqNo: SequenceNumber = _
  private var enqueuedTime: Instant = _
  private var isInclusive: Boolean = _

  private[common] def convert: EventPosition = {
    filterType match {
      case FilterType.Offset         => EventPosition.fromOffset(offset, isInclusive)
      case FilterType.SequenceNumber => EventPosition.fromSequenceNumber(seqNo, isInclusive)
      case FilterType.EnqueuedTime   => EventPosition.fromEnqueuedTime(enqueuedTime)
    }
  }

  private[common] def getFilterType: FilterType = filterType
}

object Position {
  private val StartOfStream: String = "-1"
  private val EndOfStream: String = "@latest"

  object FilterType extends Enumeration {
    type FilterType = Value
    val Offset, SequenceNumber, EnqueuedTime = Value
  }

  /**
   * Creates a position at the given offset. By default, the specified event is not included.
   * Set isInclusive to true for the specified event to be included.
   *
   * @param offset is the byte offset of the event.
   * @param isInclusive will include the specified event when set to true; otherwise, the next event is returned.
   * @return An [[Position]] instance.
   */
  def fromOffset(offset: String, isInclusive: Boolean = false): Position = {
    new Position(offset, isInclusive)
  }

  /**
   * Creates a position at the given sequence number. By default, the specified event is not included.
   * Set isInclusive to true for the specified event to be included.
   *
   * @param seqNo is the sequence number of the event.
   * @param isInclusive will include the specified event when set to true; otherwise, the next event is returned.
   * @return An [[Position]] instance.
   */
  def fromSequenceNumber(seqNo: SequenceNumber, isInclusive: Boolean = false): Position = {
    new Position(seqNo, isInclusive)
  }

  /**
   * Creates a position at the given [[Instant]]
   *
   * @param enqueuedTime is the enqueued time of the specified event.
   * @return An [[Position]] instance.
   */
  def fromEnqueuedTime(enqueuedTime: Instant): Position = {
    new Position(enqueuedTime)
  }

  /**
   * Returns the position for the start of a stream. Provide this position to your [[EventHubsConf]] to start
   * receiving from the first available event in the partition.
   *
   * @return An [[Position]] instance.
   */
  def fromStartOfStream(): Position = {
    new Position(StartOfStream, true)
  }

  /**
   * Returns the position for the end of a stream. Provide this position to your [[EventHubsConf]] to start
   * receiving from the next available event in the partition after the receiver is created.
   *
   * @return An [[Position]] instance.
   */
  def fromEndOfStream(): Position = {
    new Position(EndOfStream, false)
  }
}
