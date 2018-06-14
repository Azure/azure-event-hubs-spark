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

import java.time.Instant
import java.util.Date

import com.microsoft.azure.eventhubs.{ EventPosition => ehep }

/**
 * Defines a position of an event in an event hub partition.
 * The position can be an Offset, Sequence Number, or EnqueuedTime.
 *
 * This event is passed to the EventHubsConf to define a starting point for your Spark job.
 */
case class EventPosition private (offset: String = null,
                                  seqNo: Long = -1L,
                                  enqueuedTime: Date = null,
                                  isInclusive: Boolean = true)
    extends Serializable {

  private[eventhubs] def convert: ehep = {
    if (offset != null) {
      ehep.fromOffset(offset, isInclusive)
    } else if (seqNo >= 0L) {
      ehep.fromSequenceNumber(seqNo, isInclusive)
    } else if (enqueuedTime != null) {
      ehep.fromEnqueuedTime(enqueuedTime.toInstant)
    } else {
      throw new IllegalStateException("No position has been set.")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: EventPosition =>
        this.offset == that.offset &&
          this.seqNo == that.seqNo &&
          this.enqueuedTime == that.enqueuedTime &&
          this.isInclusive == that.isInclusive
      case _ => false
    }
  }
}

/**
 * Companion object to help create [[EventPosition]] instances.
 */
object EventPosition {

  /**
   * Creates a position at the given offset. When using EventHubs with Spark,
   * starting positions are always inclusive. Ending positions are always exclusive.
   * @param offset is the byte offset of the event.
   * @return An [[EventPosition]] instance.
   */
  def fromOffset(offset: String): EventPosition = {
    EventPosition(offset)
  }

  /**
   * Creates a position at the given sequence number. When using EventHubs with Spark,
   * starting positions are always inclusive. Ending positions are always exclusive.
   *
   * @param seqNo is the sequence number of the event.
   * @return An [[EventPosition]] instance.
   */
  def fromSequenceNumber(seqNo: SequenceNumber): EventPosition = {
    require(seqNo >= 0L, "Please pass a positive sequence number.")
    EventPosition(seqNo = seqNo)
  }

  /**
   * Creates a position at the given [[Instant]]
   *
   * @param enqueuedTime is the enqueued time of the specified event.
   * @return An [[EventPosition]] instance.
   */
  def fromEnqueuedTime(enqueuedTime: Instant): EventPosition = {
    EventPosition(enqueuedTime = Date.from(enqueuedTime))
  }

  /**
   * Returns the position for the start of a stream. Provide this position to your
   * [[EventHubsConf]] to start receiving from the first available event in the partition.
   *
   * @return An [[EventPosition]] instance.
   */
  def fromStartOfStream: EventPosition = {
    EventPosition(StartOfStream)
  }

  /**
   * Returns the position for the end of a stream. Provide this position to your
   * [[EventHubsConf]] to start receiving from the next available event in the
   * partition after the receiver is created.
   *
   * @return An [[EventPosition]] instance.
   */
  def fromEndOfStream: EventPosition = {
    EventPosition(EndOfStream, isInclusive = false)
  }
}
