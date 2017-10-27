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

import org.apache.spark.eventhubs.common.client.EventHubsClientWrapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{ DataSourceRegister, StreamSourceProvider }
import org.apache.spark.sql.types._

private[sql] class EventHubsSourceProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with Logging {

  override def shortName(): String = "eventhubs"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    (shortName(), EventHubsSourceProvider.sourceSchema(parameters))
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    // TODO: use serviceLoader EH client dependency injection
    EventHubsClientWrapper.userAgent =
      s"Structured-Streaming-${sqlContext.sparkSession.sparkContext.version}"
    new EventHubsSource(sqlContext, parameters, EventHubsClientWrapper.apply)
  }
}

private[sql] object EventHubsSourceProvider extends Serializable {

  private[eventhubs] def ifContainsPropertiesAndUserDefinedKeys(
      parameters: Map[String, String]): (Boolean, Seq[String]) = {
    val containsProperties =
      parameters.getOrElse("eventhubs.sql.containsProperties", "false").toBoolean
    val userDefinedKeys = {
      if (parameters.contains("eventhubs.sql.userDefinedKeys")) {
        parameters("eventhubs.sql.userDefinedKeys").split(",").toSeq
      } else {
        Seq()
      }
    }
    (containsProperties, userDefinedKeys)
  }

  def sourceSchema(parameters: Map[String, String]): StructType = {
    val (containsProperties, userDefinedKeys) = ifContainsPropertiesAndUserDefinedKeys(parameters)
    StructType(
      Seq(
        StructField("body", BinaryType),
        StructField("offset", LongType),
        StructField("seqNumber", LongType),
        StructField("enqueuedTime", LongType),
        StructField("publisher", StringType),
        StructField("partitionKey", StringType)
      ) ++ {
        if (containsProperties) {
          if (userDefinedKeys.nonEmpty) {
            userDefinedKeys.map(key => StructField(key, StringType))
          } else {
            Seq(
              StructField("properties", MapType(StringType, StringType, valueContainsNull = true)))
          }
        } else {
          Seq()
        }
      })
  }
}
