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

package org.apache.spark.eventhubscommon

private[spark] case class EventHubNameAndPartition(eventHubName: String, partitionId: Int) {

  private var hash: Option[Int] = None

  override def toString: String = s"$eventHubName-partition-$partitionId"

  override def hashCode(): Int = {
    if (hash.isEmpty) {
      val prime = 31
      var result = 1
      result = prime * result + partitionId
      result = prime * result + eventHubName.hashCode
      hash = Some(result)
    }
    hash.get
  }

  override def equals(obj: Any): Boolean = {
    if (this.eq(obj.asInstanceOf[Object])) {
      true
    } else if (obj == null) {
      false
    } else if (getClass != obj.getClass) {
      false
    } else {
      val otherEhNameAndPartition = obj.asInstanceOf[EventHubNameAndPartition]
      if (otherEhNameAndPartition.partitionId != partitionId) {
        false
      } else {
        if (eventHubName != otherEhNameAndPartition.eventHubName) {
          return false
        }
        true
      }
    }
  }
}

private[spark] object EventHubNameAndPartition {
  def fromString(str: String): EventHubNameAndPartition = {
    val Array(name, partition) = str.split("-partition-")
    EventHubNameAndPartition(name, partition.toInt)
  }
}
