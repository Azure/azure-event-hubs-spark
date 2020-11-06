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

package org.apache.spark.sql.eventhubs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ AnalysisException, SparkSession }
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{ BinaryType, StringType }
import org.apache.spark.util.Utils

/**
 * The [[EventHubsWriter]] class is used to write data from a batch query
 * or structured streaming query, given by a [[QueryExecution]], to EventHubs.
 * The data being sent must have a body column which contains the event
 * payloads. The body column must be a String or Binary.
 * Optionally, a partition column and partitionKey column can be set. Though
 * both cannot be set at the same time. If a partition column is present, the
 * event will be sent to the partition present in the column. If a partitionKey
 * column is present, events will be sent using the partition keys present in
 * the column. If neither are set, then the events are sent to the service and
 * are stored in round-robin fashion across all partitions.
 */
private[eventhubs] object EventHubsWriter extends Logging {

  val BodyAttributeName = "body"
  val PartitionKeyAttributeName = "partitionKey"
  val PartitionIdAttributeName = "partition"
  val PartitionIdAttributeNameAlias = "partitionId"
  val PropertiesAttributeName = "properties"

  override def toString: String = "EventHubsWriter"

  private def validateQuery(schema: Seq[Attribute], parameters: Map[String, String]): Unit = {
    schema
      .find(_.name == BodyAttributeName)
      .getOrElse(
        throw new AnalysisException(s"Required attribute '$BodyAttributeName' not found.")
      )
      .dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(
          s"$BodyAttributeName attribute type " +
            s"must be a String or BinaryType.")
    }
  }

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      parameters: Map[String, String]
  ): Unit = {
    val schema = queryExecution.analyzed.output
    validateQuery(schema, parameters)
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask = new EventHubsWriteTask(parameters, schema)
      Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
        finallyBlock = writeTask.close()
      )
    }
  }
}
