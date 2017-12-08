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

import java.lang.reflect.Constructor

import com.microsoft.azure.eventhubs.EventData
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.{ Data, MessageAnnotations }
import org.apache.qpid.proton.message.Message
import org.apache.qpid.proton.message.Message.Factory
import org.apache.spark.eventhubs.common.{
  EnqueueTime,
  EventHubsConf,
  NameAndPartition,
  PartitionId,
  SequenceNumber
}
import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
 */
private[spark] class EventHubsTestUtils extends Logging {
  //
}

private[spark] object EventHubsTestUtils {
  val PartitionCount = 4
  val EventsPerPartition = 5000
  val EventPayload = "test_event"
  val StartingSequenceNumber: SequenceNumber = 0
}

private[spark] class SimulatedEventHubsPartition {
  import EventHubsTestUtils._
  import com.microsoft.azure.eventhubs.amqp.AmqpConstants._

  private var currentSeqNo = 0

  // This allows us to invoke the EventData(Message) constructor
  private val constructor = classOf[EventData].getDeclaredConstructor(classOf[Message])
  constructor.setAccessible(true)

  private val data = for {
    id <- 0 until EventsPerPartition
    obj: AnyRef = id.toLong.asInstanceOf[AnyRef]
    msgAnnotations = new MessageAnnotations(Map(SEQUENCE_NUMBER -> obj).asJava)

    body = new Data(new Binary(s"${EventPayload}_$id".getBytes("UTF-8")))

    msg = Factory.create(null, null, msgAnnotations, null, null, body, null)
  } yield constructor.newInstance(msg)

  private[spark] def setStartingSeqNo(seqNo: SequenceNumber) = { currentSeqNo = seqNo.toInt }

  private[spark] def size = data.size

  private[spark] def get: EventData = {
    require(currentSeqNo < size, "get: SimulatedEventHubsPartition is empty.")
    val event = data(currentSeqNo)
    currentSeqNo += 1
    event
  }
}

class SimulatedEventHubs {
  import EventHubsTestUtils._

  private val partitions: Map[PartitionId, SimulatedEventHubsPartition] =
    (for { p <- 0 until PartitionCount } yield p -> new SimulatedEventHubsPartition).toMap

  def setStartingSeqNos(seqNo: SequenceNumber): Unit = {
    for (partitionId <- partitions.keySet) {
      partitions(partitionId).setStartingSeqNo(seqNo)
    }
  }

  def receive(eventCount: Int, partitionId: Int): java.lang.Iterable[EventData] = {
    (for { _ <- 0 until eventCount } yield partitions(partitionId).get).asJava
  }
}

class SimulatedClient extends Client { self =>

  import EventHubsTestUtils._

  val eventHubs: SimulatedEventHubs = new SimulatedEventHubs

  var partitionId: Int = _

  override private[spark] def createReceiver(partitionId: String,
                                             startingSeqNo: SequenceNumber): Unit = {
    self.partitionId = partitionId.toInt
    eventHubs.setStartingSeqNos(startingSeqNo)
  }

  override private[spark] def receive(eventCount: Int): java.lang.Iterable[EventData] = {
    eventHubs.receive(eventCount, self.partitionId)
  }

  override private[spark] def setPrefetchCount(count: Int): Unit = {
    // not prefetching anything
  }

  override def earliestSeqNo(eventHubNameAndPartition: NameAndPartition): SequenceNumber = {
    0L
  }

  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    EventsPerPartition
  }

  override def lastEnqueuedTime(eventHubNameAndPartition: NameAndPartition): EnqueueTime = {
    // We test the translate method in EventHubsClientWrapperSuite. We'll stick to SequenceNumbers
    // for other tests which will suffice if translate is working properly.
    0L
  }

  override def translate[T](ehConf: EventHubsConf): Map[PartitionId, SequenceNumber] = {
    (for { partitionId <- 0 until PartitionCount } yield
      partitionId -> StartingSequenceNumber).toMap
  }

  override def partitionCount: PartitionId = PartitionCount

  override def close(): Unit = {
    // nothing to close
  }
}

object SimulatedClient {
  def apply(ehConf: EventHubsConf): SimulatedClient = new SimulatedClient
}
