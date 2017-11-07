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

package org.apache.spark.eventhubs.common

/**
 * interface representing the bridge between EventHubs and Spark-side processing engine
 * (Direct DStream or Structured Streaming)
 */
private[spark] trait EventHubsConnector {

  // the id of the stream which is mapped from eventhubs instance
  def streamId: Int

  // uniquely identify the entities in eventhubs side, it can be the EventHubs namespace or the
  // combination of namespace and eventhubs name and streamId
  def uid: String

  // the list of eventhubs partitions connecting with this connector
  def namesAndPartitions: List[NameAndPartition]
}
