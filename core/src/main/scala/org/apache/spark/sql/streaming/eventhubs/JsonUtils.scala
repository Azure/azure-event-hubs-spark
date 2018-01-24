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

import org.apache.spark.eventhubs.NameAndPartition
import org.apache.spark.eventhubs._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.control.NonFatal

private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Read NameAndPartitions from json string
   */
  def partitions(str: String): Array[NameAndPartition] = {
    try {
      Serialization
        .read[Map[String, Seq[PartitionId]]](str)
        .flatMap {
          case (name, parts) =>
            parts.map { part =>
              new NameAndPartition(name, part)
            }
        }
        .toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"ehNameA":[0,1],"ehNameB":[0,1]}, got $str""")
    }
  }

  /**
   * Write NameAndPartitions as json string
   */
  def partitions(partitions: Iterable[NameAndPartition]): String = {
    val result = new mutable.HashMap[String, List[PartitionId]]
    partitions.foreach { nAndP =>
      val parts: List[PartitionId] = result.getOrElse(nAndP.ehName, Nil)
      result += nAndP.ehName -> (nAndP.partitionId :: parts)
    }
    Serialization.write(result)
  }

  /**
   * Write per-NameAndPartition seqNos as json string
   */
  def partitionSeqNos(partitionSeqNos: Map[NameAndPartition, SequenceNumber]): String = {
    val result = new mutable.HashMap[String, mutable.HashMap[PartitionId, SequenceNumber]]()
    implicit val ordering = new Ordering[NameAndPartition] {
      override def compare(x: NameAndPartition, y: NameAndPartition): Int = {
        Ordering
          .Tuple2[String, PartitionId]
          .compare((x.ehName, x.partitionId), (y.ehName, y.partitionId))
      }
    }
    val partitions = partitionSeqNos.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { nAndP =>
      val seqNo = partitionSeqNos(nAndP)
      val parts = result.getOrElse(nAndP.ehName, new mutable.HashMap[PartitionId, SequenceNumber])
      parts += nAndP.partitionId -> seqNo
      result += nAndP.ehName -> parts
    }
    Serialization.write(result)
  }

  /**
   * Read per-NameAndPartition seqNos from json string
   */
  def partitionSeqNos(jsonStr: String): Map[NameAndPartition, SequenceNumber] = {
    try {
      Serialization.read[Map[String, Map[PartitionId, SequenceNumber]]](jsonStr).flatMap {
        case (name, partSeqNos) =>
          partSeqNos.map {
            case (part, seqNo) =>
              NameAndPartition(name, part) -> seqNo
          }
      }
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"failed to parse $jsonStr" +
            s"""Expected e.g. {"ehName":{"0":23,"1":-1},"ehNameB":{"0":-2}}""")
    }
  }
}
