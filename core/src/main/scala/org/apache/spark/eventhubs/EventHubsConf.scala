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
 *
 * @param connectionString a valid connection string which will be used to connect to
 *                         an EventHubs instance. A connection string can be obtained from
 *                         the Azure portal or by using [[ConnectionStringBuilder]].
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

  /**
   * Converts the [[EventHubsConf]] instance to a [[Map[String,String]]].
   *
   * @return a [[Map[String,String]] with all the current settings in the [[EventHubsConf]]
   */
  def toMap: Map[String, String] = {
    CaseInsensitiveMap(settings.asScala.toMap)
  }

  /**
   * Clones your [[EventHubsConf]] instance.
   *
   * @return a copy of your [[EventHubsConf]]
   */
  override def clone: EventHubsConf = {
    val newConf = EventHubsConf(self.connectionString)
    newConf.settings.putAll(self.settings)
    newConf
  }

  /**
   * Sets the connection string which will be used to connect to
   * an EventHubs instance. Connection strings can be obtained from
   * the Azure portal or using the [[ConnectionStringBuilder]].
   *
   * @param connectionString a valid connection string
   * @return the updated [[EventHubsConf]] instance
   */
  def setConnectionString(connectionString: String): EventHubsConf = {
    set(ConnectionStringKey, connectionString)
  }

  /**
   * Sets the name of your EventHub instance. The connection string used to
   * construct the [[EventHubsConf]] is updated as well.
   *
   * @param name the name of an EventHub instance
   * @return the updated [[EventHubsConf]] instance
   */
  def setName(name: String): EventHubsConf = {
    val newConnStr = ConnectionStringBuilder(connectionString).setEventHubName(name).toString
    setConnectionString(newConnStr)
  }

  /** The currently set EventHub name */
  def name: String = ConnectionStringBuilder(connectionString).getEventHubName

  /** Set the consumer group for your EventHubs instance. If no consumer
   * group is provided, then [[DefaultConsumerGroup]] will be used.
   *
   * @param consumerGroup the consumer group to be used
   * @return the updated [[EventHubsConf]] instance
   */
  def setConsumerGroup(consumerGroup: String): EventHubsConf = {
    set(ConsumerGroupKey, consumerGroup)
  }

  /** The currently set consumer group. */
  def consumerGroup: Option[String] = {
    self.get(ConsumerGroupKey)
  }

  /**
   * Sets the default starting position for all partitions.
   *
   * If you would like to start from a different position for a specific partition,
   * please see [[setStartingPositions()]]. If a position is set for particiular partition,
   * we will use that position instead of the one set by this method.
   *
   * If no starting position is set, then [[DefaultEventPosition]] is used
   * (i.e. we will start from the beginning of the EventHub partition.)
   *
   * @param eventPosition the default position to start receiving events from.
   * @return the updated [[EventHubsConf]] instance
   * @see [[EventPosition]]
   */
  def setStartingPosition(eventPosition: EventPosition): EventHubsConf = {
    set(StartingPositionKey, Serialization.write(eventPosition))
  }

  /**
   * The currently set starting position.
   * @see [[EventPosition]]
   */
  def startingPosition: Option[EventPosition] = {
    self.get(StartingPositionKey) map Serialization.read[EventPosition]
  }

  /**
   * Sets starting positions on a per partition basis. This takes precedent over all
   * other configurations. If nothing is set here, then we will defer to what has been set
   * in [[setStartingPosition()]]. If nothing is set in [[setStartingPosition()]], then
   * we will start consuming from the start of the EventHub partition.
   *
   * @param eventPositions a map of parition ids (ints) to [[EventPosition]]s
   * @return the updated [[EventHubsConf]] instance
   * @see [[EventPosition]]
   */
  def setStartingPositions(eventPositions: Map[PartitionId, EventPosition]): EventHubsConf = {
    set(StartingPositionsKey, Serialization.write(eventPositions))
  }

  /**
   * The currently set positions for particular partitions.
   * @see [[EventPosition]]
   */
  def startingPositions: Option[Map[PartitionId, EventPosition]] = {
    self.get(StartingPositionsKey) map Serialization.read[Map[PartitionId, EventPosition]]
  }

  /**
   * maxRatePerPartition defines an upper bound for how many events will be
   * in a partition per batch. This method sets a max rate per partition for
   * all partitions.
   *
   * If you would like to set a different max rate for a particular partition,
   * then see [[setMaxRatesPerPartition()]]. If a max rate per partition has been set
   * using [[setMaxRatesPerPartition()]], that rate will be used. If nothing has been
   * set, then the max rate set by this method will be used. If nothing has been
   * set by the user, then we will use [[DefaultMaxRatePerPartition]].
   *
   * @param rate the default maxRatePerPartition for all partitions.
   * @return the updated [[EventHubsConf]] instance
   */
  def setMaxRatePerPartition(rate: Rate): EventHubsConf = {
    set(MaxRatePerPartitionKey, rate)
  }

  /** The currently set max rate per partition.  */
  def maxRatePerPartition: Option[Rate] = {
    self.get(MaxRatePerPartitionKey) map (_.toRate)
  }

  /**
   * For information on maxRatePerPartition, see [[setMaxRatePerPartition()]].
   *
   * This method allows users to set max rates per partition on a per partition basis.
   * If a maxRatePerPartition is set here, it will be used. If one isn't set, then the value
   * set in [[setMaxRatePerPartition()]] will be used. If no value has been set in either
   * setter method, then we will use [[DefaultMaxRatePerPartition]].
   *
   * @param rates a map of partition ids (ints) to their desired rates.
   * @return
   */
  def setMaxRatesPerPartition(rates: Map[PartitionId, Rate]): EventHubsConf = {
    set(MaxRatesPerPartitionKey, Serialization.write(rates))
  }

  /** A map of partition/max rate pairs that have been set by the user.  */
  def maxRatesPerPartition: Option[Map[PartitionId, Rate]] = {
    self.get(MaxRatesPerPartitionKey) map Serialization.read[Map[PartitionId, Rate]]
  }

  /**
   * Set the receiver timeout. We will try to receive the expected batch for
   * the length of this timeout.
   * Default: [[DefaultReceiverTimeout]]
   *
   * @param d the new receiver timeout
   * @return the updated [[EventHubsConf]] instance
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
   *
   * @param d the new operation timeout
   * @return the updated [[EventHubsConf]] instance
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

  /**
   * Rate limit on maximum number of events processed per trigger interval.
   * Only valid for Structured Streaming. The specified total number of events
   * will be proportionally split across partitions of different volume.
   *
   * @param limit the maximum number of events to be processed per trigger interval
   * @return the updated [[EventHubsConf]] instance
   */
  def setMaxSeqNosPerTrigger(limit: Long): EventHubsConf = {
    set(MaxSeqNosPerTriggerKey, limit)
  }

  // The simulated client (and simulated eventhubs) will be used. These
  // can be found in EventHubsTestUtils.
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
