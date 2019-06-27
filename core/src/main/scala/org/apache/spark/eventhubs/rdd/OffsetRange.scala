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

package org.apache.spark.eventhubs.rdd

import org.apache.spark.eventhubs.NameAndPartition
import org.apache.spark.eventhubs._
import org.apache.spark.streaming.eventhubs.EventHubsDirectDStream

import scala.language.implicitConversions

/**
 * Represents any object that has a collection of [[OffsetRange]]s.
 * This can be used to access the offset ranges in RDDs generated
 * by the [[EventHubsDirectDStream]].
 *
 * {{{
 *   EventHubsUtils.createDirectStream(...).foreachRDD { rdd =>
 *      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
 *      ...
 *   }
 * }}}
 */
trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

/**
 * Represents a sequence number range for a single partition. The range
 * generally corresponds to an inclusive starting point and exclusive
 * ending point for a single partition within an [[EventHubsRDD]].
 *
 * @param nameAndPartition the Event Hub name and Event Hub partition
 *                         associated with this offset range
 * @param fromSeqNo an inclusive starting sequence number
 * @param untilSeqNo an exclusive ending sequence number
 * @param preferredLoc the preferred executor for this partition
 */
final class OffsetRange(val nameAndPartition: NameAndPartition,
                        val fromSeqNo: SequenceNumber,
                        val untilSeqNo: SequenceNumber,
                        val preferredLoc: Option[String])
    extends Serializable {
  import OffsetRange.OffsetRangeTuple

  def name: String = nameAndPartition.ehName

  def partitionId: Int = nameAndPartition.partitionId

  def count: Long = untilSeqNo - fromSeqNo

  override def equals(obj: Any): Boolean = obj match {
    case that: OffsetRange =>
      this.name == that.name &&
        this.partitionId == that.partitionId &&
        this.fromSeqNo == that.fromSeqNo &&
        this.untilSeqNo == that.untilSeqNo
    case _ => false
  }

  override def hashCode(): Rate = {
    toTuple.hashCode()
  }

  def toTuple: OffsetRangeTuple = (nameAndPartition, fromSeqNo, untilSeqNo, preferredLoc)

  override def toString =
    s"OffsetRange(name: $name | partition: $partitionId | fromSeqNo: $fromSeqNo | untilSeqNo: $untilSeqNo)"
}

/**
 * Companion object that allows the creation of [[OffsetRange]]s.
 */
object OffsetRange {
  type OffsetRangeTuple = (NameAndPartition, SequenceNumber, SequenceNumber, Option[String])

  def apply(name: String,
            partitionId: PartitionId,
            fromSeq: SequenceNumber,
            untilSeq: SequenceNumber,
            preferredLoc: Option[String]): OffsetRange = {
    OffsetRange(NameAndPartition(name, partitionId), fromSeq, untilSeq, preferredLoc)
  }

  def apply(nAndP: NameAndPartition,
            fromSeq: SequenceNumber,
            untilSeq: SequenceNumber,
            preferredLoc: Option[String]): OffsetRange = {
    new OffsetRange(nAndP, fromSeq, untilSeq, preferredLoc)
  }

  def apply(tuple: OffsetRangeTuple): OffsetRange = {
    tupleToOffsetRange(tuple)
  }

  implicit def tupleToOffsetRange(tuple: OffsetRangeTuple): OffsetRange =
    OffsetRange(tuple._1, tuple._2, tuple._3, tuple._4)

  implicit def tupleListToOffsetRangeList(list: List[OffsetRangeTuple]): List[OffsetRange] =
    for { tuple <- list } yield tupleToOffsetRange(tuple)
}
