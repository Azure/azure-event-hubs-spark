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

import org.apache.spark.Partition
import org.apache.spark.eventhubs.NameAndPartition
import org.apache.spark.eventhubs.{ PartitionId, SequenceNumber }

private class EventHubsRDDPartition(val index: Int,
                                    val nameAndPartition: NameAndPartition,
                                    val fromSeqNo: SequenceNumber,
                                    val untilSeqNo: SequenceNumber,
                                    val preferredLoc: Option[String])
    extends Partition {

  /** Number of messages this partition refers to */
  def count: Long = untilSeqNo - fromSeqNo

  /** The EventHubs name corresponding to this RDD Partition */
  def name: String = nameAndPartition.ehName

  /** The EventHubs partition corresponding to this RDD Partition */
  def partitionId: PartitionId = nameAndPartition.partitionId
}
