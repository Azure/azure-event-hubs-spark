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

package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.{ EventHubsConf, NameAndPartition, SequenceNumber }
import org.apache.spark.eventhubs.client.CachedReceiver

/**
 * Simulated version of the cached receivers.
 */
private[spark] object SimulatedCachedReceiver extends CachedReceiver {

  import EventHubsTestUtils._

  /**
   * Grabs a single event from the [[SimulatedEventHubs]].
   *
   * @param ehConf       the Event Hubs specific parameters
   * @param nAndP        the event hub name and partition that will be consumed from
   * @param requestSeqNo the starting sequence number
   * @param batchSize    the number of events to be consumed for the RDD partition
   * @return the consumed event
   */
  override def receive(ehConf: EventHubsConf,
                       nAndP: NameAndPartition,
                       requestSeqNo: SequenceNumber,
                       batchSize: Int): EventData = {
    eventHubs(ehConf.name).receive(1, nAndP.partitionId, requestSeqNo).iterator().next()
  }
}
