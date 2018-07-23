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