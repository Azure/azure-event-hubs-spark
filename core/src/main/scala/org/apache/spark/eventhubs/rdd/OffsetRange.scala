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

import scala.language.implicitConversions

trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

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
    s"OffsetRange(partition: ${nameAndPartition.partitionId} | fromSeqNo: $fromSeqNo | untilSeqNo: $untilSeqNo)"
}

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
