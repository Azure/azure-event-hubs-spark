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

package org.apache.spark.eventhubs.common

private[spark] final class NameAndPartition(val ehName: String, val partitionId: PartitionId)
    extends Serializable {
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: NameAndPartition =>
      this.ehName == that.ehName &&
        this.partitionId == that.partitionId
    case _ => false
  }

  override def toString: String = s"$ehName-$partitionId"

  override def hashCode(): Rate = {
    toTuple.hashCode()
  }

  def toTuple: (String, PartitionId) = (ehName, partitionId)
}

private[spark] object NameAndPartition {
  def apply(ehName: String, partitionId: PartitionId) = new NameAndPartition(ehName, partitionId)

  def fromString(str: String): NameAndPartition = {
    val Array(name, partition) = str.split("-")
    NameAndPartition(name, partition.toInt)
  }
}
