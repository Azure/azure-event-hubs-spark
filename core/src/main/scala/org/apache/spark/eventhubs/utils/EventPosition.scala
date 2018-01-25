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

package org.apache.spark.eventhubs.utils

import java.time.Instant

import com.microsoft.azure.eventhubs.{ EventPosition => ehep }
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.SequenceNumber

/**
 * Defines a position of an event in an event hub partition.
 * The position can be an Offset, Sequence Number, or EnqueuedTime.
 *
 * This event is passed to the EventHubsConf to define a starting point for your Spark job.
 */
class EventPosition extends Serializable {

  import EventPosition._
  import FilterType.FilterType

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
  private[eventhubs] var seqNo: SequenceNumber = _
  private var enqueuedTime: Instant = _
  private var isInclusive: Boolean = _

  private[eventhubs] def convert: ehep = {
    filterType match {
      case FilterType.Offset         => ehep.fromOffset(offset, isInclusive)
      case FilterType.SequenceNumber => ehep.fromSequenceNumber(seqNo, isInclusive)
      case FilterType.EnqueuedTime   => ehep.fromEnqueuedTime(enqueuedTime)
    }
  }

  private[eventhubs] def getFilterType: FilterType = filterType
}

object EventPosition {
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
   * @return An [[EventPosition]] instance.
   */
  def fromOffset(offset: String, isInclusive: Boolean = false): EventPosition = {
    new EventPosition(offset, isInclusive)
  }

  /**
   * Creates a position at the given sequence number. By default, the specified event is not included.
   * Set isInclusive to true for the specified event to be included.
   *
   * @param seqNo is the sequence number of the event.
   * @param isInclusive will include the specified event when set to true; otherwise, the next event is returned.
   * @return An [[EventPosition]] instance.
   */
  def fromSequenceNumber(seqNo: SequenceNumber, isInclusive: Boolean = false): EventPosition = {
    new EventPosition(seqNo, isInclusive)
  }

  /**
   * Creates a position at the given [[Instant]]
   *
   * @param enqueuedTime is the enqueued time of the specified event.
   * @return An [[EventPosition]] instance.
   */
  def fromEnqueuedTime(enqueuedTime: Instant): EventPosition = {
    new EventPosition(enqueuedTime)
  }

  /**
   * Returns the position for the start of a stream. Provide this position to your [[EventHubsConf]] to start
   * receiving from the first available event in the partition.
   *
   * @return An [[EventPosition]] instance.
   */
  def fromStartOfStream(): EventPosition = {
    new EventPosition(StartOfStream, true)
  }

  /**
   * Returns the position for the end of a stream. Provide this position to your [[EventHubsConf]] to start
   * receiving from the next available event in the partition after the receiver is created.
   *
   * @return An [[EventPosition]] instance.
   */
  def fromEndOfStream(): EventPosition = {
    new EventPosition(EndOfStream, false)
  }
}
