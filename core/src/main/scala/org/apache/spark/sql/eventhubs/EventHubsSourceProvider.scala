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

import java.util.Locale

import org.apache.spark.eventhubs.{ EventHubsConf, _ }
import org.apache.spark.eventhubs.client.{ Client, EventHubsClient }
import org.apache.spark.eventhubs.utils.SimulatedClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext, SaveMode }
import org.apache.spark.sql.execution.streaming.{ Sink, Source }
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * The provider class for the [[EventHubsSource]].
 */
private[sql] class EventHubsSourceProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with RelationProvider
    with CreatableRelationProvider
    with Logging {

  import EventHubsConf._

  override def shortName(): String = "eventhubs"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    (shortName(), EventHubsSourceProvider.eventHubsSchema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-${sqlContext.sparkSession.sparkContext.version}"

    val caseInsensitiveParameters = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }

    new EventHubsSource(sqlContext,
                        parameters,
                        clientFactory(caseInsensitiveParameters),
                        metadataPath)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-${sqlContext.sparkSession.sparkContext.version}"

    val caseInsensitiveMap = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    new EventHubsRelation(sqlContext, parameters, clientFactory(caseInsensitiveMap))
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-${sqlContext.sparkSession.sparkContext.version}"

    val caseInsensitiveMap = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }

    new EventHubsSink(sqlContext, caseInsensitiveMap, clientFactory(caseInsensitiveMap))
  }

  override def createRelation(outerSQLContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-${outerSQLContext.sparkSession.sparkContext.version}"

    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(
          s"Save mode $mode not allowed for EventHubs. " +
            s"Allowed save modes are ${SaveMode.Append} and " +
            s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }

    val caseInsensitiveMap = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }

    EventHubsWriter.write(outerSQLContext.sparkSession,
                          data.queryExecution,
                          caseInsensitiveMap,
                          clientFactory(caseInsensitiveMap))

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for EventHubs. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException(
          "BaseRelation from EventHubs write " +
            "operation is not usable.")
    }
  }

  private def clientFactory(caseInsensitiveParams: Map[String, String]): EventHubsConf => Client = {
    if (caseInsensitiveParams
          .getOrElse(UseSimulatedClientKey.toLowerCase, DefaultUseSimulatedClient)
          .toBoolean) {
      SimulatedClient.apply
    } else {
      EventHubsClient.apply
    }
  }
}

private[sql] object EventHubsSourceProvider extends Serializable {
  def eventHubsSchema: StructType = {
    StructType(
      Seq(
        StructField("body", BinaryType),
        StructField("offset", StringType),
        StructField("sequenceNumber", LongType),
        StructField("enqueuedTime", TimestampType),
        StructField("publisher", StringType),
        StructField("partitionKey", StringType)
      ))
  }
}
