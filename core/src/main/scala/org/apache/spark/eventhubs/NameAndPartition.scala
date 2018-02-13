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

import org.json4s.jackson.Serialization

case class NameAndPartition(ehName: String, partitionId: Int) extends Serializable { self =>

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: NameAndPartition =>
      this.ehName == that.ehName &&
        this.partitionId == that.partitionId
    case _ => false
  }

  override def toString: String = {
    Serialization.write(self)
  }

  override def hashCode(): Rate = {
    toTuple.hashCode()
  }

  def toTuple: (String, Int) = (ehName, partitionId)
}

private[eventhubs] object NameAndPartition {
  def fromString(str: String): NameAndPartition = {
    Serialization.read[NameAndPartition](str)
  }
}
