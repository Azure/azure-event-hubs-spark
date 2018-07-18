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

import org.apache.qpid.proton.amqp.{
  Binary,
  Decimal128,
  Decimal32,
  Decimal64,
  Symbol,
  UnsignedByte,
  UnsignedInteger,
  UnsignedLong,
  UnsignedShort
}
import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ ArrayBasedMapData, DateTimeUtils }
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.jackson.Serialization

import scala.language.postfixOps
import collection.JavaConverters._

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

    val rdd = new EventHubsRDD(sqlContext.sparkContext, ehConf, offsetRanges)
      .mapPartitionsWithIndex { (p, iter) =>
        {
          iter.map { ed =>
            InternalRow(
              ed.getBytes,
              UTF8String.fromString(p.toString),
              UTF8String.fromString(ed.getSystemProperties.getOffset),
              ed.getSystemProperties.getSequenceNumber,
              DateTimeUtils.fromJavaTimestamp(
                new java.sql.Timestamp(ed.getSystemProperties.getEnqueuedTime.toEpochMilli)),
              UTF8String.fromString(ed.getSystemProperties.getPublisher),
              UTF8String.fromString(ed.getSystemProperties.getPartitionKey),
              ArrayBasedMapData(
                ed.getProperties.asScala
                  .mapValues {
                    case b: Binary =>
                      val buf = b.asByteBuffer()
                      val arr = new Array[Byte](buf.remaining)
                      buf.get(arr)
                      arr.asInstanceOf[AnyRef]
                    case d128: Decimal128    => d128.asBytes.asInstanceOf[AnyRef]
                    case d32: Decimal32      => d32.getBits.asInstanceOf[AnyRef]
                    case d64: Decimal64      => d64.getBits.asInstanceOf[AnyRef]
                    case s: Symbol           => s.toString.asInstanceOf[AnyRef]
                    case ub: UnsignedByte    => ub.toString.asInstanceOf[AnyRef]
                    case ui: UnsignedInteger => ui.toString.asInstanceOf[AnyRef]
                    case ul: UnsignedLong    => ul.toString.asInstanceOf[AnyRef]
                    case us: UnsignedShort   => us.toString.asInstanceOf[AnyRef]
                    case c: Character        => c.toString.asInstanceOf[AnyRef]
                    case default             => default
                  }
                  .map { p =>
                    UTF8String.fromString(p._1) -> UTF8String.fromString(Serialization.write(p._2))
                  })
            )
          }
        }
      }

    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = false).rdd
  }
}
