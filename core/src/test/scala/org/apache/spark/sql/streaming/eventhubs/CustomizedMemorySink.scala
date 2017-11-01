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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{ Append, Complete, Update }
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class CustomizedMemorySink(val schema: StructType, outputMode: OutputMode)
    extends Sink
    with Logging {

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.map(_.data).flatten
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized { batches.lastOption.toSeq.flatten(_.data) }

  def toDebugString: String = synchronized {
    batches
      .map {
        case AddedData(batchId, data) =>
          val dataStr = try data.mkString(" ")
          catch {
            case NonFatal(_) => "[Error converting to string]"
          }
          s"$batchId: $dataStr"
      }
      .mkString("\n")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      println(s"added batch $batchId to memory sink")
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case Append | Update =>
          val rows = AddedData(batchId, data.collect())
          synchronized { batches += rows }

        case Complete =>
          val rows = AddedData(batchId, data.collect())
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySink")
      }
    } else {
      println(s"Skipping already committed batch: $batchId")
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString: String = "MemorySink"
}
