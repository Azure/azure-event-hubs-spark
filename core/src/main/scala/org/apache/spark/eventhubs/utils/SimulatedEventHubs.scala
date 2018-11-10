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
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties
import org.apache.spark.eventhubs.{ PartitionId, SequenceNumber }

/**
 * Simulated EventHubs instance. All partitions are empty on creation.
 *
 * @param name the name of the [[SimulatedEventHubs]]
 * @param partitionCount the number of partitions in the [[SimulatedEventHubs]]
 */
private[spark] class SimulatedEventHubs(val name: String, val partitionCount: Int) {

  import scala.collection.JavaConverters._

  private val partitions: Map[PartitionId, SimulatedEventHubsPartition] =
    (for { p <- 0 until partitionCount } yield p -> new SimulatedEventHubsPartition).toMap

  private var count = 0

  /**
   * The number of events in a specific partition.
   *
   * @param partition the partition to be queried
   * @return the number of events in the partition
   */
  def partitionSize(partition: PartitionId): Int = {
    partitions(partition).size
  }

  /**
   * The total number of events across all partitions.
   *
   * @return the number events in the entire [[SimulatedEventHubs]]
   */
  def totalSize: Int = {
    var totalSize = 0
    for (part <- partitions.keySet) {
      totalSize += partitions(part).size
    }
    totalSize
  }

  /**
   * Receive an iterable of [[EventData]].
   *
   * @param eventCount the number of events to consume
   * @param partition the partition the events are consumed from
   * @param seqNo the starting sequence number in the partition
   * @return the batch of [[EventData]]
   */
  def receive(eventCount: Int,
              partition: Int,
              seqNo: SequenceNumber): java.lang.Iterable[EventData] = {
    (for { i <- 0 until eventCount } yield partitions(partition).get(seqNo + i)).asJava
  }

  /**
   * Send events to a [[SimulatedEventHubs]]. This send API is used
   * by [[EventHubsTestUtils]] to populate [[SimulatedEventHubs]]
   * instances.
   *
   * @param partition the partition to send events to
   * @param events the events being sent
   * @param properties the [[ApplicationProperties]] added to each event
   * @return the [[EventData]] that was sent to the [[SimulatedEventHubs]]
   */
  private[utils] def send(partition: Option[PartitionId],
                          events: Seq[Int],
                          properties: Option[Map[String, Object]] = None): Seq[Int] = {
    if (partition.isDefined) {
      partitions(partition.get).send(events, properties)
    } else {
      for (event <- events) {
        synchronized {
          val part = count % this.partitionCount
          count += 1
          this.send(Some(part), Seq(event), properties)
        }
      }
    }
    events
  }

  def clear(): Unit = synchronized {
    partitions.foreach(_._2.clear())
  }

  /**
    * Send events to a [[SimulatedEventHubs]]. This send API is used in the
    * [[SimulatedClient]] which needs send capabilities for writing stream
    * and batch queries in Structured Streaming.
    *
    * @param partition the partition to send events to
    * @param event the event being sent
    * @param properties Optional properties of the event
    */
  private[utils] def send(partition: Option[PartitionId],
                          event: EventData,
                          properties: Option[Map[String, Object]]): Unit = {
    if (partition.isDefined) {
      synchronized(partitions(partition.get).send(event, properties))
    } else {
      synchronized {
        val part = count % this.partitionCount
        count += 1
        this.send(Some(part), event, properties)
      }
    }
  }

  /**
    * Send events to a [[SimulatedEventHubs]]. This send API is used in the
    * [[SimulatedClient]] which needs send capabilities for writing stream
    * and batch queries in Structured Streaming.
    *
    * @param partition the partition to send events to
    * @param event the event being sent
    */
  private[utils] def send(partition: PartitionId,
                          event: EventData): Unit = {
    synchronized(partitions(partition).send(event))
  }

  /**
   * The earliest sequence number in a partition.
   *
   * @param partition the partition to be queried
   * @return the earliest sequence number in the partition
   */
  def earliestSeqNo(partition: PartitionId): SequenceNumber = {
    partitions(partition).earliestSeqNo
  }

  /**
   * The latest sequence number in a partition.
   *
   * @param partition the partition to be queried
   * @return the latest sequence number in the partition
   */
  def latestSeqNo(partition: PartitionId): SequenceNumber = {
    partitions(partition).latestSeqNo
  }

  /**
   * All partitions in the [[SimulatedEventHubs]] instance.
   *
   * @return [[SimulatedEventHubsPartition]]s mapped to their partition
   */
  def getPartitions: Map[PartitionId, SimulatedEventHubsPartition] = {
    partitions
  }

  override def toString: String = {
    var str = s"""
                 |EventHub Name: $name
                 |Partition Count: $partitionCount
    """.stripMargin + "\n"

    for (p <- partitions.keySet.toSeq.sorted) {
      str += s"""
        Partition: $p
        ${partitions(p).getEvents.map(_.getBytes.map(_.toChar).mkString)})
      """
      str += "\n"
    }
    str
  }

  /**
   * Represents a partition within a [[SimulatedEventHubs]]. Each
   * partition contains true [[EventData]] objects as they would
   * exist if they were sent from the service.
   */
  private[utils] class SimulatedEventHubsPartition {

    private var data: Seq[EventData] = Seq.empty

    /**
     * All the [[EventData]] in the partition.
     *
     * @return all events in the partition
     */
    def getEvents: Seq[EventData] = data

    /**
     * Appends sent events to the partition.
     *
     * @param events events being sent
     * @param properties [[ApplicationProperties]] to append to each event
     */
    private[utils] def send(events: Seq[Int], properties: Option[Map[String, Object]]): Unit = synchronized {
      for (event <- events) {
        val seqNo = data.size.toLong
        val e = EventHubsTestUtils.createEventData(s"$event".getBytes("UTF-8"), seqNo, properties)
        data = data :+ e
      }
    }


    /**
     * Appends sent events to the partition.
     *
     * @param event event being sent
     */
    private[utils] def send(event: EventData, properties: Option[Map[String, Object]] = None): Unit = {
      // Need to add a Seq No to the EventData to properly simulate the service.
      val e = EventHubsTestUtils.createEventData(event.getBytes, data.size.toLong, properties)
      synchronized(data = data :+ e)
    }

    /**
     * The number of events in the partition.
     *
     * @return the number of event in the partition.
     */
    private[spark] def size = synchronized(data.size)

    /**
     * Lookup a specific event in the partition.
     *
     * @param index the sequence number to lookup
     * @return the event
     */
    private[utils] def get(index: SequenceNumber): EventData = {
      data(index.toInt)
    }

    /**
     * The earliest sequence number in the partition.
     *
     * @return the earliest sequence number
     */
    private[utils] def earliestSeqNo: SequenceNumber = {
      if (data.isEmpty) {
        0L
      } else {
        data.map(_.getSystemProperties.getSequenceNumber).min
      }
    }

    /**
     * The latest sequence number in the partition.
     *
     * @return the latest sequence number
     */
    private[utils] def latestSeqNo: SequenceNumber = {
      if (data.isEmpty) {
        0L
      } else {
        1L + data.map(_.getSystemProperties.getSequenceNumber).max
      }
    }

    /**
      * Clears the buffer
      */
    private[utils] def clear(): Unit = data = Seq.empty
  }
}
