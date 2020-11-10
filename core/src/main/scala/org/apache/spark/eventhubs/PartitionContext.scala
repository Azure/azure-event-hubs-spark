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

package org.apache.spark.eventhubs

import java.net.URI
import org.json4s.jackson.Serialization

/**
 * Partition context which provides EventHub's information for partitions
 * to be used in Throttling Status Plugin
 *
 * @param namespaceEndpoint Namespace endpoint.
 * @param eventHubName EventHub name.
 */
class PartitionContext(val namespaceEndpoint: URI, val eventHubName: String) extends Serializable {
  override def toString: String = {
    s"NamespaceEndpoint: $namespaceEndpoint, eventHubName: $eventHubName"
  }
}
