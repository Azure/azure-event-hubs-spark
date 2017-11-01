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

import scala.reflect.ClassTag

import org.apache.spark.eventhubs.common.utils.EventHubsTestUtilities
import org.apache.spark.sql.execution.streaming._

/** A trait for actions that can be performed while testing a streaming DataFrame. */
trait StreamAction

case class AddDataToEventHubs[T: ClassTag, U: ClassTag](
    eventHubsParameters: Map[String, String],
    highestBatchId: Long = 0,
    eventPayloadsAndProperties: Seq[(T, Seq[U])] = Seq.empty[(T, Seq[U])])
    extends StreamAction
    with Serializable {

  def addData(query: Option[StreamExecution]): (Source, Offset) = {
    val sources = query.get.logicalPlan.collect {
      case StreamingExecutionRelation(source, _) if source.isInstanceOf[EventHubsSource] =>
        source.asInstanceOf[EventHubsSource]
    }
    if (sources.isEmpty) {
      throw new Exception(
        "Could not find EventHubs source in the StreamExecution logical plan to add data to")
    } else if (sources.size > 1) {
      throw new Exception(
        "Could not select the EventHubs source in the StreamExecution logical plan as there" +
          "are multiple EventHubs sources:\n\t" + sources.mkString("\n\t"))
    }
    val eventHubsSource = sources.head
    val eventHubs = EventHubsTestUtilities.getOrCreateSimulatedEventHubs(eventHubsParameters)
    EventHubsTestUtilities.addEventsToEventHubs(eventHubs, eventPayloadsAndProperties)
    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)
    val targetOffsetPerPartition = highestOffsetPerPartition.map {
      case (ehNameAndPartition, (offset, _, _)) => (ehNameAndPartition, offset)
    }
    val eventHubsBatchRecord = EventHubsBatchRecord(highestBatchId, targetOffsetPerPartition)
    (eventHubsSource, eventHubsBatchRecord)
  }
}
