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
import org.apache.spark.eventhubs.utils.EventPosition.FilterType

import scala.collection.JavaConverters._

/**
 * Test classes used to simulate an EventHubs instance.
 *
 * In main directory (instead of test) so we can inject SimulatedClient
 * in our DStream and Source tests.
 */
private[spark] class EventHubsTestUtils {
  import EventHubsTestUtils._

  def send(data: Seq[Int]): Seq[Int] = {
    var count = 0
    for (event <- data) {
      val part = count % PartitionCount
      count += 1
      EventHubsTestUtils.eventHubs.send(part, Seq(event))
    }

    data
  }

  def send(partitionId: PartitionId, data: Int*): Seq[Int] = {
    EventHubsTestUtils.eventHubs.send(partitionId, data)
    data
  }

  def getLatestSeqNos(ehConf: EventHubsConf): Map[NameAndPartition, SequenceNumber] = {
    val eventHubs = EventHubsTestUtils.eventHubs
    (for {
      p <- 0 until PartitionCount
      n = ehConf.name
      seqNo = eventHubs.latestSeqNo(p)
    } yield NameAndPartition(n, p) -> seqNo).toMap
  }

  def createEventHubs(): Unit = {
    if (EventHubsTestUtils.eventHubs == null) {
      EventHubsTestUtils.eventHubs = new SimulatedEventHubs(PartitionCount)
    } else {
      throw new IllegalStateException
    }
  }

  def destroyEventHubs(): Unit = {
    if (EventHubsTestUtils.eventHubs != null) {
      EventHubsTestUtils.eventHubs = null
    }
  }
}

private[spark] object EventHubsTestUtils {
  var PartitionCount: Int = 4
  var MaxRate: Rate = 5
  var ConnectionString = ConnectionStringBuilder()
    .setNamespaceName("namespace")
    .setEventHubName("name")
    .setSasKeyName("keyName")
    .setSasKey("key")
    .toString

  private[spark] var eventHubs: SimulatedEventHubs = _

  def setDefaults(): Unit = {
    PartitionCount = 4
    MaxRate = 5
  }

  def sendEvents(partitionId: PartitionId, events: Int*): Unit = {
    eventHubs.send(partitionId, events)
  }
}

/**
 * Simulated EventHubs instance. All partitions are empty on creation. Must use
 * [[EventHubsTestUtils.sendEvents()]] in order to populate the instance.
 */
private[spark] class SimulatedEventHubs(val partitionCount: Int) {

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
private[spark] class SimulatedClient extends Client { self =>

  import EventHubsTestUtils._

  var partitionId: Int = _
  var currentSeqNo: SequenceNumber = _

  override private[spark] def createReceiver(partitionId: String,
                                             startingSeqNo: SequenceNumber): Unit = {
    self.partitionId = partitionId.toInt
    self.currentSeqNo = startingSeqNo
  }

  override private[spark] def receive(eventCount: Int): java.lang.Iterable[EventData] = {
    val events = eventHubs.receive(eventCount, self.partitionId, currentSeqNo)
    currentSeqNo += eventCount
    events
  }

  override private[spark] def setPrefetchCount(count: Int): Unit = {
    // not prefetching anything in tests
  }

  override def earliestSeqNo(partitionId: PartitionId): SequenceNumber = {
    EventHubsTestUtils.eventHubs.earliestSeqNo(partitionId)
  }

  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    eventHubs.latestSeqNo(partitionId)
  }

  // TODO: implement simulated methods used in translate, and then remove this method.
  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int): Map[PartitionId, SequenceNumber] = {
    val positions = ehConf.startingPositions.getOrElse(Map.empty)
    require(positions.size == partitionCount)
    require(positions.forall(x => x._2.getFilterType == FilterType.SequenceNumber))

    positions.mapValues(_.seqNo).mapValues { seqNo =>
      { if (seqNo == -1L) 0L else seqNo }
    }
  }

  override def partitionCount: PartitionId = EventHubsTestUtils.eventHubs.partitionCount

  override def close(): Unit = {
    // nothing to close
  }
}

private[spark] object SimulatedClient {
  def apply(ehConf: EventHubsConf): SimulatedClient = new SimulatedClient
}
