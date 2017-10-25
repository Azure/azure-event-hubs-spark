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

package org.apache.spark.sql.streaming.eventhubs

import org.apache.spark.eventhubs.common.NameAndPartition
import org.apache.spark.sql.execution.streaming.Offset
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.control.NonFatal

// the descriptor of EventHubsBatchRecord to communicate with StreamExecution
private[streaming] case class EventHubsBatchRecord(batchId: Long,
                                                   targetSeqNums: Map[NameAndPartition, Long])
    extends Offset {
  override def json: String = JsonUtils.partitionAndSeqNum(batchId, targetSeqNums)
}

private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def partitionAndSeqNum(batchId: Long, seqNums: Map[NameAndPartition, Long]): String = {
    val convertedStringIndexedMap = new mutable.HashMap[String, Long]
    seqNums.foreach {
      case (eventHubNameAndPartition, offsetAndSeqNum) =>
        convertedStringIndexedMap += eventHubNameAndPartition.toString -> offsetAndSeqNum
    }
    Serialization.write((batchId, convertedStringIndexedMap.toMap))
  }

  def partitionAndSeqNum(jsonStr: String): EventHubsBatchRecord = {
    try {
      val deserializedTuple = Serialization.read[(Int, Map[String, Long])](jsonStr)
      val batchId = deserializedTuple._1
      EventHubsBatchRecord(batchId, deserializedTuple._2.map {
        case (ehNameAndPartitionStr, seqNum) =>
          (NameAndPartition.fromString(ehNameAndPartitionStr), seqNum)
      })
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(s"failed to parse $jsonStr")
    }
  }

  def partitionOffsetAndSeqNums(batchId: Long,
                                offsets: Map[NameAndPartition, (Long, Long)]): String = {
    val convertedStringIndexedMap = new mutable.HashMap[String, (Long, Long)]
    offsets.foreach {
      case (eventHubNameAndPartition, offsetAndSeqNum) =>
        convertedStringIndexedMap += eventHubNameAndPartition.toString -> offsetAndSeqNum
    }
    Serialization.write((batchId, convertedStringIndexedMap))
  }
}
