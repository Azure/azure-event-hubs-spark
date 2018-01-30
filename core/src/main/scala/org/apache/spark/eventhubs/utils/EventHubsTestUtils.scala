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
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.{ Data, MessageAnnotations }
import org.apache.qpid.proton.message.Message
import org.apache.qpid.proton.message.Message.Factory
import org.apache.spark.eventhubs.{ EventHubsConf, NameAndPartition }
import org.apache.spark.eventhubs.client.Client
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
    var count = 0
    val eventHub = eventHubs(ehName)
    for (event <- data) {
      val part = count % eventHub.partitionCount
      count += 1
      eventHub.send(part, Seq(event))
    }

    data
  }

  def send(ehName: String, partitionId: PartitionId, data: Seq[Int]): Seq[Int] = {
    eventHubs(ehName).send(partitionId, data)
    data
  }

  def getLatestSeqNos(ehConf: EventHubsConf): Map[NameAndPartition, SequenceNumber] = {
    val eventHubs = EventHubsTestUtils.eventHubs
    (for {
      p <- 0 until DefaultPartitionCount
      n = ehConf.name
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
    val connectionString = ConnectionStringBuilder()
      .setNamespaceName("namespace")
      .setEventHubName(ehName)
      .setSasKeyName("keyName")
      .setSasKey("key")
      .build

    val positions: Map[PartitionId, EventPosition] = (for {
      partitionId <- 0 until DefaultPartitionCount
    } yield partitionId -> EventPosition.fromSequenceNumber(0L, isInclusive = true)).toMap

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
}

/**
 * Simulated EventHubs instance. All partitions are empty on creation.
 */
private[spark] class SimulatedEventHubs(val name: String, val partitionCount: Int) {

  private val partitions: Map[PartitionId, SimulatedEventHubsPartition] =
    (for { p <- 0 until partitionCount } yield p -> new SimulatedEventHubsPartition).toMap

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

  def send(partitionId: PartitionId, events: Seq[Int]): Unit = {
    partitions(partitionId).send(events)
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

  /** Specifies the contents of each partition. */
  private[spark] class SimulatedEventHubsPartition {
    import com.microsoft.azure.eventhubs.amqp.AmqpConstants._

    private var data: Seq[EventData] = Seq.empty

    def getEvents: Seq[EventData] = data

    // This allows us to invoke the EventData(Message) constructor
    private val constructor = classOf[EventData].getDeclaredConstructor(classOf[Message])
    constructor.setAccessible(true)

    private[spark] def send(events: Seq[Int]): Unit = {
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

        data = data :+ constructor.newInstance(msg)
      }
    }

    private[spark] def size = data.size

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

  private var partitionId: Int = _
  private var currentSeqNo: SequenceNumber = _
  private val eventHub = eventHubs(ehConf.name)

  override def createReceiver(partitionId: String, startingSeqNo: SequenceNumber): Unit = {
    self.partitionId = partitionId.toInt
    self.currentSeqNo = startingSeqNo
  }

  override def receive(eventCount: Int): java.lang.Iterable[EventData] = {
    val events = eventHub.receive(eventCount, self.partitionId, currentSeqNo)
    currentSeqNo += eventCount
    events
  }

  override def setPrefetchCount(count: Int): Unit = {
    // not prefetching anything in tests
  }

  override def earliestSeqNo(partitionId: PartitionId): SequenceNumber = {
    eventHub.earliestSeqNo(partitionId)
  }

  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    eventHub.latestSeqNo(partitionId)
  }

  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int): Map[PartitionId, SequenceNumber] = {

    val positions = ehConf.startingPositions.getOrElse(Map.empty)

    if (positions.isEmpty) {
      val position = ehConf.startingPosition.get
      require(position.seqNo >= 0L)

      (for { id <- 0 until partitionCount } yield id -> position.seqNo).toMap
    } else {
      require(positions.forall(x => x._2.seqNo >= 0L))
      require(positions.size == partitionCount)

      positions.mapValues(_.seqNo).mapValues { seqNo =>
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
