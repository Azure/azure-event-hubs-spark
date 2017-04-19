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

import org.apache.spark.eventhubscommon.utils.{EventHubsTestUtilities, SimulatedEventHubs}
import org.apache.spark.sql.execution.streaming._

/** A trait for actions that can be performed while testing a streaming DataFrame. */
trait StreamAction

case class EventHubsAddDataMemory[A](source: MemoryStream[A], data: Seq[A])
  extends EventHubsAddData {
  override def toString: String = s"AddData to $source: ${data.mkString(",")}"

  override def addData(query: Option[StreamExecution]): (Source, Offset) = {
    (source, source.addData(data))
  }
}

/**
 * Adds the given data to the stream. Subsequent check answers will block
 * until this data has been processed.
 */
object EventHubsAddData {
  def apply[A](source: MemoryStream[A], data: A*): EventHubsAddDataMemory[A] =
    EventHubsAddDataMemory(source, data)
}

/** A trait that can be extended when testing a source. */
trait EventHubsAddData extends StreamAction with Serializable {
  /**
    * Called to adding the data to a source. It should find the source to add data to from
    * the active query, and then return the source object the data was added, as well as the
    * offset of added data.
    */
  def addData(query: Option[StreamExecution]): (Source, Offset)
}

case class AddEventHubsData[T: ClassTag, U: ClassTag](
     eventHubsParameters: Map[String, String], highestBatchId: Long = 0,
     eventPayloadsAndProperties: Seq[(T, Seq[U])] = Seq.empty[(T, Seq[U])])
  extends EventHubsAddData {

  override def addData(query: Option[StreamExecution]): (Source, Offset) = {

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
    val eventHubs = EventHubsTestUtilities.getOrSimulateEventHubs(eventHubsParameters)

    EventHubsTestUtilities.addEventsToEventHubs(eventHubs, eventPayloadsAndProperties)

    val highestOffsetPerPartition = EventHubsTestUtilities.getHighestOffsetPerPartition(eventHubs)

    val targetOffsetPerPartition = highestOffsetPerPartition.map(x => x._1 -> x._2._2)
    val eventHubsBatchRecord = EventHubsBatchRecord(highestBatchId, targetOffsetPerPartition)
    (eventHubsSource, eventHubsBatchRecord)
  }
}