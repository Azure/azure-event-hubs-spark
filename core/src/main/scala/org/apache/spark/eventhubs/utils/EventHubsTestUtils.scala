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

import java.util.Date

import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventhubs.impl.AmqpConstants.{
  ENQUEUED_TIME_UTC,
  OFFSET,
  SEQUENCE_NUMBER
}
import com.microsoft.azure.eventhubs.impl.EventDataImpl
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.{ ApplicationProperties, Data, MessageAnnotations }
import org.apache.qpid.proton.message.Message
import org.apache.qpid.proton.message.Message.Factory
import org.apache.spark.eventhubs.{ EventHubsConf, NameAndPartition }
import org.apache.spark.eventhubs.client.{ CachedReceiver, Client }
import org.apache.spark.eventhubs._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * In order to better test the Event Hubs + Spark connector,
 * this utility file has a simulated, in-memory Event Hubs
 * instance. This doesn't fully simulate the Event Hubs
 * service. Rather, it only simulates the the functionality
 * needed to do proper unit and integration tests for the
 * connector.
 *
 * This specific utility class provides the ability to create,
 * send to, and destroy a simulated Event Hubs (as well as some
 * other similar utility functions).
 */
private[spark] class EventHubsTestUtils {

  import EventHubsTestUtils._

  /**
   * Sends events to the specified simulated event hub.
   *
   * @param ehName the event hub to send to
   * @param partition the partition id that will receive the data
   * @param data the data being sent
   * @param properties additional application properties
   * @return
   */
  def send(ehName: String,
           partition: Option[PartitionId] = None,
           data: Seq[Int],
           properties: Option[Map[String, Object]] = None): Seq[Int] = {
    eventHubs(ehName).send(partition, data, properties)
  }

  /**
   * Returns the latest sequence numbers for all partitions of the Event Hub
   * specified in the provided [[EventHubsConf]].
   *
   * @param ehConf the configuration containing all Event Hub related info
   * @return the latest sequence numbers for all partitions in the simulated
   *         event hub
   */
  def getLatestSeqNos(ehConf: EventHubsConf): Map[NameAndPartition, SequenceNumber] = {
    val n = ehConf.name
    (for {
      p <- 0 until eventHubs(n).partitionCount
      seqNo = eventHubs(n).latestSeqNo(p)
    } yield NameAndPartition(n, p) -> seqNo).toMap
  }

  /**
   * Retrieves the simulated event hubs.
   *
   * @param ehName the name of the event hub
   * @return the [[SimulatedEventHubs]] instance
   */
  def getEventHubs(ehName: String): SimulatedEventHubs = {
    eventHubs(ehName)
  }

  /**
   * Creates a [[SimulatedEventHubs]].
   *
   * @param ehName the name of the simulated event hub
   * @param partitionCount the number of partitions in the simulated event hub
   * @return the newly created [[SimulatedEventHubs]]
   */
  def createEventHubs(ehName: String, partitionCount: Int): SimulatedEventHubs = {
    EventHubsTestUtils.eventHubs.put(ehName, new SimulatedEventHubs(ehName, partitionCount))
    eventHubs(ehName)
  }

  /**
   * Destroys the the event hub if it is present.
   * @param ehName the name of the simulated event hub to be destroyed.
   */
  def destroyEventHubs(ehName: String): Unit = {
    eventHubs.remove(ehName)
  }

  /**
   * Destroys all simulated event hubs.
   */
  def destroyAllEventHubs(): Unit = {
    eventHubs.clear
  }

  /**
   * Creates a generic [[EventHubsConf]] with dummy information.
   *
   * @param ehName the simulated event hub name to be used in this [[EventHubsConf]].
   *               If this isn't correctly provided, all lookups for the simulated
   *               event hubs will fail.
   * @return the [[EventHubsConf]] with dummy information
   */
  def getEventHubsConf(ehName: String = "name"): EventHubsConf = {
    val partitionCount = getEventHubs(ehName).partitionCount

    val connectionString = ConnectionStringBuilder()
      .setNamespaceName("namespace")
      .setEventHubName(ehName)
      .setSasKeyName("keyName")
      .setSasKey("key")
      .build

    val positions: Map[NameAndPartition, EventPosition] = (for {
      partition <- 0 until partitionCount
    } yield NameAndPartition(ehName, partition) -> EventPosition.fromSequenceNumber(0L)).toMap

    EventHubsConf(connectionString)
      .setConsumerGroup("consumerGroup")
      .setStartingPositions(positions)
      .setMaxRatePerPartition(DefaultMaxRate)
      .setUseSimulatedClient(true)
  }

  /**
   * Sends the same number of events to every partition in the
   * simulated Event Hubs. All partitions will get identical
   * data.
   *
   * Let's say we pass a count of 10. 10 events will be sent to
   * each partition. The payload of each event is a simple
   * counter. In this example, the payloads would be 0, 1, 2, 3,
   * 4, 5, 6, 7, 8, 9.
   *
   * @param ehName the simulated event hub to receive the events
   * @param count the number of events to be generated for each
   *              partition
   * @param properties the [[ApplicationProperties]] to be inserted
   *                   to each event
   */
  def populateUniformly(ehName: String,
                        count: Int,
                        properties: Option[Map[String, Object]] = None): Unit = {
    val eventHub = eventHubs(ehName)
    for (i <- 0 until eventHub.partitionCount) {
      eventHub.send(Some(i), 0 until count, properties)
    }
  }
}

/**
 * A companion class containing:
 * - Constants used in testing
 * - Simple utility methods
 * - A mapping of Event Hub names to [[SimulatedEventHubs]] instances.
 */
private[spark] object EventHubsTestUtils {
  val DefaultPartitionCount: Int = 4
  val DefaultMaxRate: Rate = 5
  val DefaultName = "name"

  private[utils] val eventHubs: mutable.Map[String, SimulatedEventHubs] = mutable.Map.empty

  def createEventData(event: Array[Byte],
                      seqNo: Long,
                      properties: Option[Map[String, Object]]): EventData = {
    val constructor = classOf[EventDataImpl].getDeclaredConstructor(classOf[Message])
    constructor.setAccessible(true)

    val s = seqNo.toLong.asInstanceOf[AnyRef]
    // This value is not accurate. However, "offet" is never used in testing.
    // Placing dummy value here because one is required in order for EventData
    // to serialize/de-serialize properly during tests.
    val o = s.toString.asInstanceOf[AnyRef]
    val t = new Date(System.currentTimeMillis()).asInstanceOf[AnyRef]

    val msgAnnotations = new MessageAnnotations(
      Map(SEQUENCE_NUMBER -> s, OFFSET -> o, ENQUEUED_TIME_UTC -> t).asJava)

    val body = new Data(new Binary(event))
    val msg = Factory.create(null, null, msgAnnotations, null, null, body, null)
    if (properties.isDefined) {
      val appProperties = new ApplicationProperties(properties.get.asJava)
      msg.setApplicationProperties(appProperties)
    }
    constructor.newInstance(msg).asInstanceOf[EventData]
  }
}

/**
 * Simulated EventHubs instance. All partitions are empty on creation.
 *
 * @param name the name of the [[SimulatedEventHubs]]
 * @param partitionCount the number of partitions in the [[SimulatedEventHubs]]
 */
private[spark] class SimulatedEventHubs(val name: String, val partitionCount: Int) {

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
    (for { _ <- 0 until eventCount } yield partitions(partition).get(seqNo)).asJava
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
                          properties: Option[Map[String, Object]]): Seq[Int] = {
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

  /**
   * Send events to a [[SimulatedEventHubs]]. This send API is used in the
   * [[SimulatedClient]] which needs send capabilities for writing stream
   * and batch queries in Structured Streaming.
   *
   * @param partition the partition to send events to
   * @param event the event being sent
   */
  private[utils] def send(partition: Option[PartitionId], event: EventData): Unit = {
    if (partition.isDefined) {
      synchronized(partitions(partition.get).send(event))
    } else {
      synchronized {
        val part = count % this.partitionCount
        count += 1
        this.send(Some(part), event)
      }
    }
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
    private[utils] def send(events: Seq[Int], properties: Option[Map[String, Object]]): Unit = {
      synchronized {
        for (event <- events) {
          val seqNo = data.size.toLong
          val e = EventHubsTestUtils.createEventData(s"$event".getBytes("UTF-8"), seqNo, properties)
          data = data :+ e
        }
      }
    }

    /**
     * Appends sent events to the partition.
     *
     * @param event event being sent
     */
    private[utils] def send(event: EventData): Unit = {
      // Need to add a Seq No to the EventData to properly simulate the service.
      val e = EventHubsTestUtils.createEventData(event.getBytes, data.size.toLong, None)
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
        data.head.getSystemProperties.getSequenceNumber
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
        data.size
      }
    }
  }
}

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

  private var rPartitionId: Int = _ // used in receivers
  private var sPartitionId: Int = _ // used in senders
  private var currentSeqNo: SequenceNumber = _
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
                    partitionKey: Option[String] = None): Unit = {
    if (partitionKey.isDefined) {
      throw new UnsupportedOperationException
    } else {
      eventHub.send(partition, event)
    }
  }

  /**
   * The earliest sequence number in a partition.
   *
   * @param partition the partition being queried
   * @return the earliest sequence number for the specified partition
   */
  override def earliestSeqNo(partition: PartitionId): SequenceNumber = {
    eventHub.earliestSeqNo(partition)
  }

  /**
   * The latest sequence number in a partition.
   *
   * @param partition the partition being queried
   * @return the latest sequence number for the specified partition
   */
  override def latestSeqNo(partition: PartitionId): SequenceNumber = {
    eventHub.latestSeqNo(partition)
  }

  /**
   * A tuple containing the earliest and latest sequence numbers
   * in a partition.
   *
   * @param partition the partition that will be queried
   * @return the earliest and latest sequence numbers for the specified partition.
   */
  override def boundedSeqNos(partition: PartitionId): (SequenceNumber, SequenceNumber) = {
    (earliestSeqNo(partition), latestSeqNo(partition))
  }

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
    val apiCall = if (useStart) { earliestSeqNo _ } else { latestSeqNo _ }

    if (positions.isEmpty && position.isEmpty) {
      (for { id <- 0 until eventHub.partitionCount } yield id -> apiCall(id)).toMap
    } else if (positions.isEmpty) {
      require(position.get.seqNo >= 0L)
      (for { id <- 0 until partitionCount } yield id -> position.get.seqNo).toMap
    } else {
      require(positions.get.forall(x => x._2.seqNo >= 0L))
      require(positions.get.size == partitionCount)
      positions.get.map { case (k, v) => k.partitionId -> v }.mapValues(_.seqNo).mapValues {
        seqNo =>
          { if (seqNo == -1L) 0L else seqNo }
      }
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

/**
 * Simulated version of the cached receivers.
 */
private[spark] object SimulatedCachedReceiver extends CachedReceiver {

  import EventHubsTestUtils._

  /**
   * Grabs a single event from the [[SimulatedEventHubs]].
   *
   * @param ehConf the Event Hubs specific parameters
   * @param nAndP the event hub name and partition that will be consumed from
   * @param requestSeqNo the starting sequence number
   * @param batchSize the number of events to be consumed for the RDD partition
   * @return the consumed event
   */
  override def receive(ehConf: EventHubsConf,
                       nAndP: NameAndPartition,
                       requestSeqNo: SequenceNumber,
                       batchSize: Int): EventData = {
    eventHubs(ehConf.name).receive(1, nAndP.partitionId, requestSeqNo).iterator().next()
  }
}
