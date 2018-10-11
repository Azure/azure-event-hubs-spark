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

import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.StructType

import scala.language.postfixOps

/**
 * A [[BaseRelation]] to allow batch queries against Event Hubs data.
 */
private[eventhubs] class EventHubsRelation(override val sqlContext: SQLContext,
                                           parameters: Map[String, String])
    extends BaseRelation
    with TableScan
    with Logging {

  import org.apache.spark.eventhubs._

  private val ehConf = EventHubsConf.toConf(parameters)
  private val eventHubClient = EventHubsSourceProvider.clientFactory(parameters)(ehConf)

  override def schema: StructType = EventHubsSourceProvider.eventHubsSchema

  override def buildScan(): RDD[Row] = {
    val partitionCount: Int = eventHubClient.partitionCount

    val fromSeqNos = eventHubClient.translate(ehConf, partitionCount)
    val untilSeqNos = eventHubClient.translate(ehConf, partitionCount, useStart = false)

    require(fromSeqNos.forall(f => f._2 >= 0L),
            "Currently only sequence numbers can be passed in your starting positions.")
    require(untilSeqNos.forall(u => u._2 >= 0L),
            "Currently only sequence numbers can be passed in your ending positions.")

    val offsetRanges = untilSeqNos.keySet.map { p =>
      val fromSeqNo = fromSeqNos
        .getOrElse(p, throw new IllegalStateException(s"$p doesn't have a fromSeqNo"))
      val untilSeqNo = untilSeqNos(p)
      OffsetRange(ehConf.name, p, fromSeqNo, untilSeqNo, None)
    }.toArray
    eventHubClient.close()

    logInfo(
      "GetBatch generating RDD of with offsetRanges: " +
        offsetRanges.sortBy(_.nameAndPartition.toString).mkString(", "))

    val rdd = EventHubsSourceProvider.toInternalRow(
      new EventHubsRDD(sqlContext.sparkContext, ehConf.trimmed, offsetRanges))
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = false).rdd
  }
}
