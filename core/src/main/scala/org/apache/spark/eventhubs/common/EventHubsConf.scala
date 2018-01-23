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

package org.apache.spark.eventhubs.common

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.eventhubs.common.utils.{ ConnectionStringBuilder, Position }
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import language.implicitConversions

/*
Need to change:
- Expose connection string builder.
- Construct EventHubsConf with a connection string. Be sure to parse the event hub name and namespace out if it b/c it's used for logging purposes.
- Remove name, namespace, key, key name APIs

---

- We should have a setStartingPositions API
  - setStartingPositions(EventPosition)
  - setStartingPositions(Map[PartitionId -> EventPosition])
    - serialize this and put it in the map.
    - Should we expose NameAndPartition?
- Remove all sequence number, offset, and enqueued time APIs.


 */

// TODO: Can we catch malformed configs at compile-time?
// TODO: What if users are starting from a checkpoint? Currently it's mandated they specify some starting point
// ^^ I suspect we'll check if a checkpoint dir has been set within the dstream and source.
//    if that's the case, then maybe we shouldn't mandate users provide a starting point.
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
final class EventHubsConf private (connectionStr: String)
    extends Serializable
    with Logging
    with Cloneable { self =>

  private implicit val formats = Serialization.formats(NoTypeHints)

  private val settings = new ConcurrentHashMap[String, String]()
  settings.put("eventhubs.connectionString", connectionStr)

  private var _maxRatesPerPartition: Map[PartitionId, Rate] = Map.empty

  private[common] def set[T](key: String, value: T): EventHubsConf = {
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

  private[spark] def isValid: Boolean = {
    true
  }

  /** Get your config in the form of a string to string map. */
  def toMap: Map[String, String] = {
    require(self.isValid)
    set("eventhubs.maxRates", EventHubsConf.maxRatesToString(_maxRatesPerPartition))

    CaseInsensitiveMap(settings.asScala.toMap)
  }

  /** Make a copy of you EventHubsConf */
  override def clone: EventHubsConf = {
    val newConf = EventHubsConf(self.connectionString)
    newConf.settings.putAll(self.settings)
    newConf._maxRatesPerPartition ++= self._maxRatesPerPartition
    newConf
  }

  def setConnectionString(connectionString: String): EventHubsConf = {
    set("eventhubs.connectionString", connectionString)
  }

  def connectionString: String = {
    self.get("eventhubs.connectionString").get
  }

  def setName(name: String): EventHubsConf = {
    val newConnStr = ConnectionStringBuilder(connectionString).setEventHubName(name).toString
    setConnectionString(newConnStr)
  }

  def name: String = ConnectionStringBuilder(connectionString).getEventHubName

  /** Set the consumer group for your EventHubs instance. */
  def setConsumerGroup(consumerGroup: String): EventHubsConf = {
    set("eventhubs.consumerGroup", consumerGroup)
  }

  /** The currently set consumer group. */
  def consumerGroup: Option[String] = {
    self.get("eventhubs.consumerGroup")
  }

  def setStartingPosition(eventPosition: Position): EventHubsConf = {
    set("eventhubs.startingPosition", Serialization.write(eventPosition))
  }

  private[spark] def startingPosition: Option[Position] = {
    self.get("eventhubs.startingPosition") map Serialization.read[Position]
  }

  def setStartingPositions(eventPositions: Map[PartitionId, Position]): EventHubsConf = {
    set("eventhubs.startingPositions", Serialization.write(eventPositions))
  }

  private[spark] def startingPositions: Option[Map[PartitionId, Position]] = {
    self.get("eventhubs.startingPositions") map Serialization.read[Map[PartitionId, Position]]
  }

  /**
   * Set the max rate per partition. This allows you to set rates on a per partition basis.
   * If you don't specify a max rate for a specific partition, we'll use [[DefaultMaxRatePerPartition]].
   *
   * Example:
   * {{{
   * // Set rate to 20 for partition 1.
   * ehConf.setMaxRatePerPartition(1 to 1, 20) // inclusive option
   * ehConf.setMaxRatePerPartition(1 until 2, 20) // exclusive option
   *
   * // Set rate to 50 for partition 4, 5, and 6.
   * ehConf.setMaxRatePerPartition(4 to 6, 50) // inclusive option
   * ehConf.setMaxRatePerPartition(4 until 7, 50) // exclusive option }}}
   */
  def setMaxRatePerPartition(range: Range, rate: Rate): EventHubsConf = {
    val newRates: Map[PartitionId, Rate] =
      (for { partitionId <- range } yield partitionId -> rate).toMap
    _maxRatesPerPartition ++= newRates
    this
  }

  /** A map of partition/max rate pairs that have been set by the user.  */
  def maxRatesPerPartition: Map[PartitionId, Rate] = {
    _maxRatesPerPartition
  }

  /**
   * Set the receiver timeout. We will try to receive the expected batch for the length of this timeout.
   * Default: [[DefaultReceiverTimeout]]
   */
  def setReceiverTimeout(d: Duration): EventHubsConf = {
    set("eventhubs.receiverTimeout", d)
  }

  /** The current receiver timeout.  */
  def receiverTimeout: Option[Duration] = {
    self.get("eventhubs.receiverTimeout") map (str => Duration.parse(str))

  }

  /**
   * Set the operation timeout. We will retry failures when contacting the EventHubs service for the length of this timeout.
   * Default: [[DefaultOperationTimeout]]
   */
  def setOperationTimeout(d: Duration): EventHubsConf = {
    set("eventhubs.operationTimeout", d)
  }

  /** The current operation timeout. */
  def operationTimeout: Option[Duration] = {
    self.get("eventhubs.operationTimeout") map (str => Duration.parse(str))
  }

  /** Set to true if you want EventHubs properties to be included in your DataFrame.  */
  def setSqlContainsProperties(b: Boolean): EventHubsConf = {
    set("eventhubs.sql.containsProperties", b)
  }

  /** Whether EventHubsConf currently contains sql properties. */
  def sqlContainsProperties: Option[Boolean] = {
    self.get("eventhubs.sql.containsProperties") map (str => str.toBoolean)
  }

  /** If your EventHubs data has user-defined keys, set them here.  */
  def setSqlUserDefinedKeys(keys: String*): EventHubsConf = {
    set("eventhubs.sql.userDefinedKeys", keys.toSet.mkString(","))
  }

  /** Current user defined keys. */
  def sqlUserDefinedKeys: Option[Array[String]] = {
    self.get("eventhubs.sql.userDefinedKeys") map (str => str.split(","))
  }

  def setFailOnDataLoss(b: Boolean): EventHubsConf = {
    set("failOnDataLoss", b)
  }

  def failOnDataLoss: Option[Boolean] = {
    self.get("eventhubs.failOnDataLoss") map (str => str.toBoolean)
  }

  def setMaxSeqNosPerTrigger(limit: Long): EventHubsConf = {
    set("maxSeqNosPerTrigger", limit)
  }

  private[spark] def setUseSimulatedClient(b: Boolean): EventHubsConf = {
    set("useSimulatedClient", b)
  }

  /** All max rates currently set will be erased. */
  def clearMaxRates(): EventHubsConf = {
    self._maxRatesPerPartition = Map.empty
    this
  }
}

object EventHubsConf extends Logging {

  /** Creates an EventHubsConf */
  def apply(connectionString: String) = new EventHubsConf(connectionString)

  private[spark] def toConf(params: Map[String, String]): EventHubsConf = {
    val ehConf = EventHubsConf(params("eventhubs.connectionString"))
    for ((k, v) <- params) {
      ehConf.set(k, v)
    }

    ehConf._maxRatesPerPartition = parseMaxRatesPerPartition(params("eventhubs.maxRates"))

    ehConf
  }

  private[common] def parseMaxRatesPerPartition(maxRates: String): Map[PartitionId, Rate] = {
    for { (k, v) <- stringToMap(maxRates) } yield k -> v.toRate
  }

  private def stringToMap(str: String): Map[PartitionId, String] = {
    if (str == "" || str == null) {
      Map.empty
    } else {
      val partitionsAndValues = str.split(",")
      (for {
        partitionAndValue <- partitionsAndValues
        partition = partitionAndValue.split(":")(0)
        value = partitionAndValue.split(":")(1)
      } yield partition.toPartitionId -> value) toMap
    }
  }

  private[common] def maxRatesToString(maxRates: Map[PartitionId, Rate]): String = {
    mapToString(maxRates)
  }

  private def mapToString(m: Map[PartitionId, _]): String = {
    val ordered = m.toSeq.sortBy(_._1)
    (for { (partition, value) <- ordered } yield s"$partition:$value").mkString(",")
  }
}
