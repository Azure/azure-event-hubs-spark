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
import org.apache.qpid.proton.amqp.messaging.{ Data, MessageAnnotations }
import org.apache.qpid.proton.message.Message
import org.apache.qpid.proton.message.Message.Factory
import org.apache.spark.eventhubs.{ EventHubsConf, NameAndPartition }
import org.apache.spark.eventhubs.client.{ CachedReceiver, Client }
import org.apache.spark.eventhubs._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Test classes used to simulate an EventHubs instance.
 *
 * In main directory (instead of test) so we can inject SimulatedClient
 * in our DStream and Source tests.
 */
private[spark] class EventHubsTestUtils {

  import EventHubsTestUtils._

  def send(ehName: String, data: Seq[Int]): Seq[Int] = {
    eventHubs(ehName).send(data)
  }

  def send(ehName: String, partitionId: PartitionId, data: Seq[Int]): Seq[Int] = {
    eventHubs(ehName).send(partitionId, data)
  }

  def getLatestSeqNos(ehConf: EventHubsConf): Map[NameAndPartition, SequenceNumber] = {
    val n = ehConf.name
    (for {
      p <- 0 until eventHubs(n).partitionCount
      seqNo = eventHubs(n).latestSeqNo(p)
    } yield NameAndPartition(n, p) -> seqNo).toMap
  }

  def getEventHubs(ehName: String): SimulatedEventHubs = {
    eventHubs(ehName)
  }

  def createEventHubs(ehName: String, partitionCount: Int): SimulatedEventHubs = {
    EventHubsTestUtils.eventHubs.put(ehName, new SimulatedEventHubs(ehName, partitionCount))
    eventHubs(ehName)
  }

  def destroyEventHubs(ehName: String): Unit = {
    eventHubs.remove(ehName)
  }

  def destroyAllEventHubs(): Unit = {
    eventHubs.clear
  }

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

  // Put 'count' events in every simulated EventHubs partition
  def populateUniformly(ehName: String, count: Int): Unit = {
    val eventHub = eventHubs(ehName)
    for (i <- 0 until eventHub.partitionCount) {
      eventHub.send(i, 0 until count)
    }
  }
}

private[spark] object EventHubsTestUtils {
  val DefaultPartitionCount: Int = 4
  val DefaultMaxRate: Rate = 5
  val DefaultName = "name"

  private[utils] val eventHubs: mutable.Map[String, SimulatedEventHubs] = mutable.Map.empty

  def createEventData(event: Array[Byte], seqNo: Long): EventData = {
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
    constructor.newInstance(msg).asInstanceOf[EventData]
  }
}

/**
 * Simulated EventHubs instance. All partitions are empty on creation.
 */
private[spark] class SimulatedEventHubs(val name: String, val partitionCount: Int) {

  private val partitions: Map[PartitionId, SimulatedEventHubsPartition] =
    (for { p <- 0 until partitionCount } yield p -> new SimulatedEventHubsPartition).toMap

  private var count = 0

  def partitionSize(partitionId: PartitionId): Int = {
    partitions(partitionId).size
  }

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

  def send(events: Seq[Int]): Seq[Int] = {
    for (event <- events) {
      synchronized {
        val part = count % this.partitionCount
        count += 1
        this.send(part, Seq(event))
      }
    }
    events
  }

  def send(partitionId: PartitionId, events: Seq[Int]): Seq[Int] = {
    partitions(partitionId).send(events)
    events
  }

  def send(event: EventData): Unit = {
    synchronized {
      val part = count % this.partitionCount
      count += 1
      this.send(part, event)
    }
  }

  def send(partitionId: PartitionId, event: EventData): Unit = {
    synchronized(partitions(partitionId).send(event))
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
  private[spark] class SimulatedEventHubsPartition {
    import com.microsoft.azure.eventhubs.impl.AmqpConstants._

    private var data: Seq[EventData] = Seq.empty

    def getEvents: Seq[EventData] = data

    // This allows us to invoke the EventData(Message) constructor
    private val constructor = classOf[EventDataImpl].getDeclaredConstructor(classOf[Message])
    constructor.setAccessible(true)

    private[spark] def send(events: Seq[Int]): Unit = {
      synchronized {
        for (event <- events) {
          val seqNo = data.size.toLong.asInstanceOf[AnyRef]

          // This value is not accurate. However, "offet" is never used in testing.
          // Placing dummy value here because one is required in order for EventData
          // to serialize/de-serialize properly during tests.
          val offset = data.size.toString.asInstanceOf[AnyRef]

          val time = new Date(System.currentTimeMillis()).asInstanceOf[AnyRef]

          val msgAnnotations = new MessageAnnotations(
            Map(SEQUENCE_NUMBER -> seqNo, OFFSET -> offset, ENQUEUED_TIME_UTC -> time).asJava)

          val body = new Data(new Binary(s"$event".getBytes("UTF-8")))

          val msg = Factory.create(null, null, msgAnnotations, null, null, body, null)

          data = data :+ constructor.newInstance(msg).asInstanceOf[EventData]
        }
      }
    }

    private[spark] def send(event: EventData): Unit = {
      // Need to add a Seq No to the EventData to properly simulate the service.
      val e = EventHubsTestUtils.createEventData(event.getBytes, data.size.toLong)
      synchronized(data = data :+ e)
    }

    private[spark] def size = synchronized(data.size)

    private[spark] def get(index: SequenceNumber): EventData = {
      data(index.toInt)
    }

    private[spark] def earliestSeqNo: SequenceNumber = {
      if (data.isEmpty) {
        0L
      } else {
        data.head.getSystemProperties.getSequenceNumber
      }
    }

    private[spark] def latestSeqNo: SequenceNumber = {
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

  override def send(event: EventData): Unit = {
    eventHub.send(event)
  }

  override def send(event: EventData, partitionKey: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def send(event: EventData, partitionId: Int): Unit = {
    eventHub.send(partitionId, event)
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
                       requestSeqNo: SequenceNumber): EventData = {
    eventHubs(ehConf.name).receive(1, nAndP.partitionId, requestSeqNo).iterator().next()
  }
}
