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

import org.apache.spark.eventhubs.common.NameAndPartition
import org.apache.spark.eventhubs.common.client.EventHubsOffsetTypes.EventHubsOffsetType

import language.implicitConversions

private[spark] case class OffsetRange(nameAndPartition: NameAndPartition,
                                      fromOffset: Long,
                                      fromSeq: Long,
                                      untilSeq: Long,
                                      offsetType: EventHubsOffsetType) {

  private[spark] def toTuple = (nameAndPartition, fromOffset, fromSeq, untilSeq, offsetType)
}

private[spark] object OffsetRange {
  type OffsetRangeTuple = (NameAndPartition, Long, Long, Long, EventHubsOffsetType)

  implicit def tupleToOffsetRange(tuple: OffsetRangeTuple): OffsetRange =
    OffsetRange(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5)

  implicit def tupleListToOffsetRangeList(list: List[OffsetRangeTuple]): List[OffsetRange] =
    for { tuple <- list } yield tupleToOffsetRange(tuple)
}
