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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types._

class EventHubsSource(userDefinedKeys: Seq[String]) extends Source {

  override def schema: StructType = {
    // in this phase, we shall include the system properties as well as the ones defined in the
    // application properties
    // TODO: do we need to add body length?
    StructType(Seq(
      StructField("body", BinaryType),
      StructField("offset", LongType),
      StructField("seqNumber", LongType),
      StructField("enqueuedTime", LongType),
      StructField("publisher", StringType),
      StructField("partitionKey", StringType)
    ) ++ userDefinedKeys.map(udkey =>
      StructField(udkey, ObjectType(classOf[Any]))))
  }

  /**
   * @return return the target offset in the next batch
   */
  override def getOffset: Option[Offset] = {
    // 1. get the highest offset
    None
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    null
  }

  override def stop(): Unit = {

  }
}
