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
import org.apache.spark.eventhubs.{ EventHubsConf, PartitionId, SequenceNumber }
import org.apache.spark.eventhubs.client.Client

/**
 * A [[Client]] which simulates a connection to the Event Hubs service.
 * The simulated client receives an [[EventHubsConf]] which contains an
 * Event Hub name. The Event Hub name is used to look up the
 * [[SimulatedEventHubs]] the [[SimulatedClient]] should "connect" to.
 *
 * @param ehConf the Event Hubs specific options
 */
private[spark] class SimulatedClient(private val ehConf: EventHubsConf) extends Client { self =>

  import EventHubsTestUtils._

  private var sPartitionId: Int = _ // used in senders
  private val eventHub = eventHubs(ehConf.name)

  /**
   * Creates a simulated partition sender.
   *
   * @param partition the partition that will receive events
   *                    from send calls.
   */
  override def createPartitionSender(partition: Int): Unit = {
    sPartitionId = partition.toInt
  }

  /**
   * Send events to a [[SimulatedEventHubs]].
   *
   * @param event the event that is being sent.
   * @param partition the partition that will receive all events being sent.
   * @param partitionKey the partitionKey will be hash'ed to determine the
   *                     partition to send the events to. On the Received
   *                     message this can be accessed at
   *                     [[EventData.SystemProperties#getPartitionKey()]]
   */
  override def send(event: EventData,
                    partition: Option[Int] = None,
                    partitionKey: Option[String] = None,
                    properties: Option[Map[String, String]]): Unit = {
    if (partitionKey.isDefined) {
      throw new UnsupportedOperationException
    } else {
      eventHub.send(partition, event, properties)
    }
  }

  /**
   * Same as boundedSeqNos, but collects info for all partitions.
   *
   * @return the earliest and latest sequence numbers for all partitions in the Event Hub
   */
  override def allBoundedSeqNos: Map[PartitionId, (SequenceNumber, SequenceNumber)] =
    (0 until partitionCount)
      .map(i => i -> ((eventHub.earliestSeqNo(i), eventHub.latestSeqNo(i)): (SequenceNumber, SequenceNumber)))
      .toMap

  /**
   * Translates starting (or ending) positions to sequence numbers.
   *
   * @param ehConf the [[EventHubsConf]] containing starting (or ending positions)
   * @param partitionCount the number of partitions in the Event Hub instance
   * @param useStart translates starting positions when true and ending positions
   *                 when false
   * @return mapping of partitions to starting positions as sequence numbers
   */
  override def translate(ehConf: EventHubsConf,
                         partitionCount: Int,
                         useStart: Boolean = true): Map[PartitionId, SequenceNumber] = {

    val positions = if (useStart) { ehConf.startingPositions } else { ehConf.endingPositions }
    val position = if (useStart) { ehConf.startingPosition } else { ehConf.endingPosition }
    val apiCall = if (useStart) { eventHub.earliestSeqNo _ } else { eventHub.latestSeqNo _ }

    if (positions.isEmpty && position.isEmpty) {
      (for { id <- 0 until eventHub.partitionCount } yield id -> apiCall(id)).toMap
    } else if (positions.isEmpty) {
      if (position.get.seqNo < 0L) {
        (for { id <- 0 until partitionCount } yield id -> apiCall(id)).toMap
      } else {
        (for { id <- 0 until partitionCount } yield id -> position.get.seqNo).toMap
      }
    } else {
      require(positions.get.forall(x => x._2.seqNo >= 0L))
      require(positions.get.size == partitionCount)
      positions.get.map { case (k, v) => k.partitionId -> v }.mapValues(_.seqNo).mapValues {
        seqNo =>
          { if (seqNo == -1L) 0L else seqNo }
      }.toMap
    }
  }

  /**
   * The number of partitions in the EventHubs instance.
   *
   * @return partition count
   */
  override val partitionCount: PartitionId = eventHub.partitionCount

  /**
   * A NOP. There's nothing to clean up in the [[SimulatedClient]].
   */
  override def close(): Unit = {
    // nothing to close
  }
}

/**
 * Companion object to help create [[SimulatedClient]] instances.
 */
private[spark] object SimulatedClient {
  def apply(ehConf: EventHubsConf): SimulatedClient = new SimulatedClient(ehConf)
}
