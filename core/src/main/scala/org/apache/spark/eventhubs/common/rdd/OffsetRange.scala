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

package org.apache.spark.eventhubs.common.rdd

import org.apache.spark.eventhubs.common._

import language.implicitConversions

trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

private[spark] final class OffsetRange(val nameAndPartition: NameAndPartition,
                                       val fromSeqNo: SequenceNumber,
                                       val untilSeqNo: SequenceNumber)
    extends Serializable {
  import OffsetRange.OffsetRangeTuple

  def name: String = nameAndPartition.ehName

  def partitionId: Int = nameAndPartition.partitionId

  def count: Long = untilSeqNo - fromSeqNo

  def toTuple: OffsetRangeTuple = (nameAndPartition, fromSeqNo, untilSeqNo)

  override def toString =
    s"OffsetRange(partitionId: ${nameAndPartition.partitionId} | fromSeqNo: $fromSeqNo | untilSeqNo: $untilSeqNo)"
}

private[spark] object OffsetRange {
  type OffsetRangeTuple = (NameAndPartition, SequenceNumber, SequenceNumber)

  def apply(name: String,
            partitionId: PartitionId,
            fromSeq: SequenceNumber,
            untilSeq: SequenceNumber): OffsetRange = {
    OffsetRange(NameAndPartition(name, partitionId), fromSeq, untilSeq)
  }

  def apply(nAndP: NameAndPartition,
            fromSeq: SequenceNumber,
            untilSeq: SequenceNumber): OffsetRange = {
    new OffsetRange(nAndP, fromSeq, untilSeq)
  }

  def apply(tuple: OffsetRangeTuple): OffsetRange = {
    tupleToOffsetRange(tuple)
  }

  implicit def tupleToOffsetRange(tuple: OffsetRangeTuple): OffsetRange =
    OffsetRange(tuple._1, tuple._2, tuple._3)

  implicit def tupleListToOffsetRangeList(list: List[OffsetRangeTuple]): List[OffsetRange] =
    for { tuple <- list } yield tupleToOffsetRange(tuple)
}
