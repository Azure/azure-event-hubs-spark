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

package org.apache.spark.eventhubs.common.utils

import java.util.Date

import com.microsoft.azure.eventhubs.EventData
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.{ Data, MessageAnnotations }
import org.apache.qpid.proton.message.Message
import org.apache.qpid.proton.message.Message.Factory
import org.apache.spark.eventhubs.common._
import org.apache.spark.eventhubs.common.client.Client

import scala.collection.JavaConverters._

/**
 * Test classes used to simulate an EventHubs instance.
 *
 * In main directory (instead of test) so we can inject SimulatedClient
 * in our DStream and Source tests.
 */
private[spark] class EventHubsTestUtils {
  import EventHubsTestUtils._

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

    // This allows us to invoke the EventData(Message) constructor
    private val constructor = classOf[EventData].getDeclaredConstructor(classOf[Message])
    constructor.setAccessible(true)

    private var data: Seq[EventData] = Seq.empty

    def getEvents: Seq[EventData] = data

    private[spark] def send(events: Seq[Int]): Unit = {

      for (event <- events) {
        val seqNo = data.size.toLong.asInstanceOf[AnyRef]

        // This value is not accurate. It's not used in testing, but a value
        // must be provided for EventData to be serialized/deserialized properly.
        val offset = data.size.toString.asInstanceOf[AnyRef]

        val time = new Date().asInstanceOf[AnyRef]

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
      data.head.getSystemProperties.getSequenceNumber
    }

    private[spark] def latestSeqNo: SequenceNumber = {
      data.last.getSystemProperties.getSequenceNumber
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

  override def earliestSeqNo(nameAndPartition: NameAndPartition): SequenceNumber = {
    EventHubsTestUtils.eventHubs.earliestSeqNo(nameAndPartition.partitionId)
  }

  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    eventHubs.latestSeqNo(partitionId)
  }

  override def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): EnqueueTime = {
    // We test the translate method in EventHubsClientWrapperSuite. We'll stick to SequenceNumbers
    // for all other tests. If the translate method works properly, then omitting EnqueueTime and
    // Byte Offsets from our RDD, DStream, and Sources tests is perfectly OK.
    0L
  }

  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int): Map[PartitionId, SequenceNumber] = {
    require(ehConf.startSequenceNumbers.nonEmpty)
    ehConf.startSequenceNumbers
  }

  override def partitionCount: PartitionId = EventHubsTestUtils.eventHubs.partitionCount

  override def close(): Unit = {
    // nothing to close
  }
}

private[spark] object SimulatedClient {
  def apply(ehConf: EventHubsConf): SimulatedClient = new SimulatedClient
}
