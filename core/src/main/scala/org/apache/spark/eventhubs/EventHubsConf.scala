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

package org.apache.spark.eventhubs

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.eventhubs.utils.{ ConnectionStringBuilder, EventPosition }
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
 * Configuration for your EventHubs instance when being used with Apache Spark.
 *
 * Namespace, name, keyName, key, and consumerGroup are required.
 *
 * EventHubsConf is case insensitive.
 *
 * You can start from the beginning of a stream, end of a stream, from particular offsets, or from
 * particular enqueue times. If none of those are provided, we will start from the beginning of your stream.
 * If more than one of those are provided, you will get a runtime error.
 */
final class EventHubsConf private (val connectionString: String)
    extends Serializable
    with Logging
    with Cloneable { self =>

  import EventHubsConf._

  private implicit val formats = Serialization.formats(NoTypeHints)

  private val settings = new ConcurrentHashMap[String, String]()
  this.setConnectionString(connectionString)

  private[eventhubs] def set[T](key: String, value: T): EventHubsConf = {
    if (key == null) {
      throw new NullPointerException("set: null key")
    }
    if (value == null) {
      throw new NullPointerException(s"set: null value for $key")
    }

    if (self.get(key.toLowerCase).isDefined) {
      logWarning(s"$key has already been set to ${self.get(key).get}. Overwriting with $value")
    }

    settings.put(key.toLowerCase, value.toString)
    this
  }

  private[spark] def apply(key: String): String = {
    get(key).get
  }

  private def get(key: String): Option[String] = {
    Option(settings.get(key.toLowerCase))
  }

  /** Get your config in the form of a string to string map. */
  def toMap: Map[String, String] = {
    CaseInsensitiveMap(settings.asScala.toMap)
  }

  /** Make a copy of you EventHubsConf */
  override def clone: EventHubsConf = {
    val newConf = EventHubsConf(self.connectionString)
    newConf.settings.putAll(self.settings)
    newConf
  }

  def setConnectionString(connectionString: String): EventHubsConf = {
    set(ConnectionStringKey, connectionString)
  }

  def setName(name: String): EventHubsConf = {
    val newConnStr = ConnectionStringBuilder(connectionString).setEventHubName(name).toString
    setConnectionString(newConnStr)
  }

  def name: String = ConnectionStringBuilder(connectionString).getEventHubName

  /** Set the consumer group for your EventHubs instance. */
  def setConsumerGroup(consumerGroup: String): EventHubsConf = {
    set(ConsumerGroupKey, consumerGroup)
  }

  /** The currently set consumer group. */
  def consumerGroup: Option[String] = {
    self.get(ConsumerGroupKey)
  }

  def setStartingPosition(eventPosition: EventPosition): EventHubsConf = {
    set(StartingPositionKey, Serialization.write(eventPosition))
  }

  def startingPosition: Option[EventPosition] = {
    self.get(StartingPositionKey) map Serialization.read[EventPosition]
  }

  def setStartingPositions(eventPositions: Map[PartitionId, EventPosition]): EventHubsConf = {
    set(StartingPositionsKey, Serialization.write(eventPositions))
  }

  def startingPositions: Option[Map[PartitionId, EventPosition]] = {
    self.get(StartingPositionsKey) map Serialization.read[Map[PartitionId, EventPosition]]
  }

  def setMaxRatePerPartition(rate: Rate): EventHubsConf = {
    set(MaxRatePerPartitionKey, rate)
  }

  /** A map of partition/max rate pairs that have been set by the user.  */
  def maxRatePerPartition: Option[Rate] = {
    self.get(MaxRatePerPartitionKey) map (_.toRate)
  }

  def setMaxRatesPerPartition(rates: Map[PartitionId, Rate]): EventHubsConf = {
    set(MaxRatesPerPartitionKey, Serialization.write(rates))
  }

  def maxRatesPerPartition: Option[Map[PartitionId, Rate]] = {
    self.get(MaxRatesPerPartitionKey) map Serialization.read[Map[PartitionId, Rate]]
  }

  /**
   * Set the receiver timeout. We will try to receive the expected batch for the length of this timeout.
   * Default: [[DefaultReceiverTimeout]]
   */
  def setReceiverTimeout(d: Duration): EventHubsConf = {
    set(ReceiverTimeoutKey, d)
  }

  /** The current receiver timeout.  */
  def receiverTimeout: Option[Duration] = {
    self.get(ReceiverTimeoutKey) map (str => Duration.parse(str))

  }

  /**
   * Set the operation timeout. We will retry failures when contacting the EventHubs service for the length of this timeout.
   * Default: [[DefaultOperationTimeout]]
   */
  def setOperationTimeout(d: Duration): EventHubsConf = {
    set(OperationTimeoutKey, d)
  }

  /** The current operation timeout. */
  def operationTimeout: Option[Duration] = {
    self.get(OperationTimeoutKey) map (str => Duration.parse(str))
  }

  def setFailOnDataLoss(b: Boolean): EventHubsConf = {
    set(FailOnDataLossKey, b)
  }

  def failOnDataLoss: Option[Boolean] = {
    self.get(FailOnDataLossKey) map (str => str.toBoolean)
  }

  def setMaxSeqNosPerTrigger(limit: Long): EventHubsConf = {
    set(MaxSeqNosPerTriggerKey, limit)
  }

  private[spark] def setUseSimulatedClient(b: Boolean): EventHubsConf = {
    set(UseSimulatedClientKey, b)
  }
}

object EventHubsConf extends Logging {

  // Option key values
  val ConnectionStringKey = "eventhubs.connectionString"
  val ConsumerGroupKey = "eventhubs.consumerGroup"
  val StartingPositionKey = "eventhubs.startingPosition"
  val StartingPositionsKey = "eventhubs.startingPositions"
  val MaxRatePerPartitionKey = "eventhubs.maxRatePerPartition"
  val MaxRatesPerPartitionKey = "eventhubs.maxRatesPerPartition"
  val ReceiverTimeoutKey = "eventhubs.receiverTimeout"
  val OperationTimeoutKey = "eventhubs.operationTimeout"
  val FailOnDataLossKey = "failOnDataLoss"
  val MaxSeqNosPerTriggerKey = "maxSeqNosPerTrigger"
  val UseSimulatedClientKey = "useSimulatedClient"

  /** Creates an EventHubsConf */
  def apply(connectionString: String) = new EventHubsConf(connectionString)

  private[spark] def toConf(params: Map[String, String]): EventHubsConf = {
    val ehConf = EventHubsConf(params("eventhubs.connectionString"))

    for ((k, v) <- params) { ehConf.set(k, v) }

    ehConf
  }
}
