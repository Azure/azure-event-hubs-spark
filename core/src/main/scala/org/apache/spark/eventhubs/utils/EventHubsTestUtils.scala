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
   * @param partitionId the partition id that will receive the data
   * @param data the data being sent
   * @param properties additional application properties
   * @return
   */
  def send(ehName: String,
           partitionId: Option[PartitionId] = None,
           data: Seq[Int],
           properties: Option[Map[String, Object]] = None): Seq[Int] = {
    eventHubs(ehName).send(partitionId, data, properties)
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
      partitionId <- 0 until partitionCount
    } yield NameAndPartition(ehName, partitionId) -> EventPosition.fromSequenceNumber(0L)).toMap

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
   * @param partitionId the partition to be queried
   * @return the number of events in the partition
   */
  def partitionSize(partitionId: PartitionId): Int = {
    partitions(partitionId).size
  }

  /**
   * @return the number events in the entire [[SimulatedEventHubs]]
   */
  def totalSize: Int = {
    var totalSize = 0
    for (part <- partitions.keySet) {
      totalSize += partitions(part).size
    }
    totalSize
  }

  def receive(eventCount: Int,
              partitionId: Int,
              seqNo: SequenceNumber): java.lang.Iterable[EventData] = {
    (for { _ <- 0 until eventCount } yield partitions(partitionId).get(seqNo)).asJava
  }

  private[utils] def send(partitionId: Option[PartitionId],
                          events: Seq[Int],
                          properties: Option[Map[String, Object]]): Seq[Int] = {
    if (partitionId.isDefined) {
      partitions(partitionId.get).send(events, properties)
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

  private[utils] def send(partitionId: Option[PartitionId], event: EventData): Unit = {
    if (partitionId.isDefined) {
      synchronized(partitions(partitionId.get).send(event))
    } else {
      synchronized {
        val part = count % this.partitionCount
        count += 1
        this.send(Some(part), event)
      }
    }
  }

  def earliestSeqNo(partitionId: PartitionId): SequenceNumber = {
    partitions(partitionId).earliestSeqNo
  }

  def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    partitions(partitionId).latestSeqNo
  }

  def getPartitions: Map[PartitionId, SimulatedEventHubsPartition] = {
    partitions
  }

  override def toString: String = {
    print(s"""
      |EventHub Name: $name
      |Partition Count: $partitionCount
    """.stripMargin)

    for (p <- partitions.keySet.toSeq.sorted) {
      print(s"""
        Partition: $p
        ${partitions(p).getEvents.map(_.getBytes.map(_.toChar).mkString)})
      """)
    }
    ""
  }

  /** Specifies the contents of each partition. */
  private[utils] class SimulatedEventHubsPartition {

    private var data: Seq[EventData] = Seq.empty

    def getEvents: Seq[EventData] = data

    // This allows us to invoke the EventData(Message) constructor
    private val constructor = classOf[EventDataImpl].getDeclaredConstructor(classOf[Message])
    constructor.setAccessible(true)

    private[utils] def send(events: Seq[Int], properties: Option[Map[String, Object]]): Unit = {
      synchronized {
        for (event <- events) {
          val seqNo = data.size.toLong
          val e = EventHubsTestUtils.createEventData(s"$event".getBytes("UTF-8"), seqNo, properties)
          data = data :+ e
        }
      }
    }

    private[utils] def send(event: EventData): Unit = {
      // Need to add a Seq No to the EventData to properly simulate the service.
      val e = EventHubsTestUtils.createEventData(event.getBytes, data.size.toLong, None)
      synchronized(data = data :+ e)
    }

    private[spark] def size = synchronized(data.size)

    private[utils] def get(index: SequenceNumber): EventData = {
      data(index.toInt)
    }

    private[utils] def earliestSeqNo: SequenceNumber = {
      if (data.isEmpty) {
        0L
      } else {
        data.head.getSystemProperties.getSequenceNumber
      }
    }

    private[utils] def latestSeqNo: SequenceNumber = {
      if (data.isEmpty) {
        0L
      } else {
        data.size
      }
    }
  }
}

/** A simulated EventHubs client. */
private[spark] class SimulatedClient(ehConf: EventHubsConf) extends Client { self =>

  import EventHubsTestUtils._

  private var rPartitionId: Int = _ // used in receivers
  private var sPartitionId: Int = _ // used in senders
  private var currentSeqNo: SequenceNumber = _
  private val eventHub = eventHubs(ehConf.name)

  override def createPartitionSender(partitionId: Int): Unit = {
    sPartitionId = partitionId.toInt
  }

  override def send(event: EventData,
                    partition: Option[Int] = None,
                    partitionKey: Option[String] = None): Unit = {
    if (partitionKey.isDefined) {
      throw new UnsupportedOperationException
    } else {
      eventHub.send(partition, event)
    }
  }

  override def earliestSeqNo(partitionId: PartitionId): SequenceNumber = {
    eventHub.earliestSeqNo(partitionId)
  }

  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    eventHub.latestSeqNo(partitionId)
  }

  override def boundedSeqNos(partitionId: PartitionId): (SequenceNumber, SequenceNumber) = {
    (earliestSeqNo(partitionId), latestSeqNo(partitionId))
  }

  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int,
                            useStarting: Boolean = true): Map[PartitionId, SequenceNumber] = {

    val positions = if (useStarting) { ehConf.startingPositions } else { ehConf.endingPositions }
    val position = if (useStarting) { ehConf.startingPosition } else { ehConf.endingPosition }
    val apiCall = if (useStarting) { earliestSeqNo _ } else { latestSeqNo _ }

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

  override def partitionCount: PartitionId = eventHub.partitionCount

  override def close(): Unit = {
    // nothing to close
  }
}

private[spark] object SimulatedClient {
  def apply(ehConf: EventHubsConf): SimulatedClient = new SimulatedClient(ehConf)
}

private[spark] object SimulatedCachedReceiver extends CachedReceiver {

  import EventHubsTestUtils._

  override def receive(ehConf: EventHubsConf,
                       nAndP: NameAndPartition,
                       requestSeqNo: SequenceNumber,
                       batchSize: Int): EventData = {
    eventHubs(ehConf.name).receive(1, nAndP.partitionId, requestSeqNo).iterator().next()
  }
}
