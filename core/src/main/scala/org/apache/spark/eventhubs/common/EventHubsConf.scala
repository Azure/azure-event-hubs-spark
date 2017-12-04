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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.collection.JavaConverters._
import language.implicitConversions

// TODO deprecate partitionCount ASAP (like don't release with it included). Get partition count from the service.
// TODO: Can we catch malformed configs at compile-time?
/**
 * Configuration for your EventHubs instance when being used with Apache Spark.
 *
 * Namespace, name, keyName, key, partitionCount, progressDirectory, and consumerGroup are required.
 *
 * You can start from the beginning of a stream, end of a stream, from particular offsets, or from
 * particular enqueue times. If none of those are provided, we will start from the beginning of your stream.
 * If more than one of those are provided, you will get a runtime error.
 */
final class EventHubsConf private extends Serializable with Logging with Cloneable {
  self =>

  private val settings = new ConcurrentHashMap[String, String]()

  private var _startOffsets: Map[PartitionId, Offset] = Map.empty

  private var _startEnqueueTimes: Map[PartitionId, EnqueueTime] = Map.empty

  private var _maxRatesPerPartition: Map[PartitionId, Rate] = Map.empty

  // Tracks what starting point was provided by the user
  // (i.e. Offset, EnqueueTime, SequenceNumber, StartOfStream, EndOfStream)
  private var startingWith: String = _

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

  private def setStartingWith(str: String): Unit = {
    startingWith = str
    set("eventhubs.startingWith", str)
  }

  private[spark] def isValid: Boolean = {
    require(
      namespace.isDefined &&
        name.isDefined &&
        keyName.isDefined &&
        key.isDefined &&
        partitionCount.isDefined &&
        progressDirectory.isDefined,
      "EventHubsConf is invalid. You must set a namespace, name, keyName, key, partitionCount, progressDirectory, and consumerGroup"
    )

    if (startOfStream.isDefined && startOfStream.get) {
      require(endOfStream.isEmpty || !endOfStream.get,
              "EventHubsConf is invalid. Don't set startOfStream and endOfStream.")
      require(startOffsets.isEmpty,
              "EventHubsConf is invalid. Don't set startOfStream and startOffsets.")
      require(startEnqueueTimes.isEmpty,
              "EventHubsConf is invalid. Don't set startOfStream and startEnqueueTimes.")
      logInfo(
        "validate: startOfStream is set to true. All partitions will start from the beginning of the stream.")
    } else if (endOfStream.isDefined && endOfStream.get) {
      require(startOffsets.isEmpty,
              "EventHubsConf is invalid. Don't set endOfStream and startOffsets.")
      require(startEnqueueTimes.isEmpty,
              "EventHubsConf is invalid. Don't set endOfStream and startEnqueueTimes.")
      logInfo(
        "validate: endOfStream is set to true. All partitions will start from the beginning of the stream.")
    } else if (startOffsets.nonEmpty) {
      require(startEnqueueTimes.isEmpty,
              "EventHubsConf is invalid. Don't set startOffsets and startEnqueueTimes.")
      logInfo(
        s"validate: startOffsets $startOffsets will be used as starting points for your stream.")
    } else if (startEnqueueTimes.nonEmpty) {
      logInfo(
        s"validate: startEnqueueTimes $startEnqueueTimes will be used as starting points for your stream.")
    } else {
      throw new IllegalArgumentException(
        "You must set a starting point for your application. You can do this with one of the following methods: " +
          "setStartOfStream, setEndOfStream, setStartOffsets, or setStartEnqueueTimes")
    }
    true
  }

  /** Get your config in the form of a string to string map. */
  def toMap: Map[String, String] = {
    require(self.isValid)
    set("eventhubs.maxRates", EventHubsConf.maxRatesToString(_maxRatesPerPartition))
    startingWith match {
      case "Offsets" => set("eventhubs.startOffsets", EventHubsConf.offsetsToString(_startOffsets))
      case "EnqueueTimes" =>
        set("eventhubs.startEnqueueTimes", EventHubsConf.enqueueTimesToString(_startEnqueueTimes))
      case _ =>
    }

    CaseInsensitiveMap(settings.asScala.toMap)
  }

  /** Make a copy of you EventHubsConf */
  override def clone: EventHubsConf = {
    val newConf = EventHubsConf()
    newConf.settings.putAll(self.settings)
    newConf._startOffsets ++= self._startOffsets
    newConf._startEnqueueTimes ++= self._startEnqueueTimes
    newConf._maxRatesPerPartition ++= self._maxRatesPerPartition
    newConf.startingWith = self.startingWith
    newConf
  }

  /** Set the namespace of your EventHubs instance. Note: this overwrites any URI that has been set. */
  def setNamespace(namespace: String): EventHubsConf = {
    set("eventhubs.namespace", namespace)
  }

  /** Set the URI of your EventHubs instance. Note: this overwrites any Namespace that has been set. */
  def setURI(uri: String): EventHubsConf = {
    setNamespace(uri)
  }

  /** Set the name of your EventHubs instance. */
  def setName(name: String): EventHubsConf = {
    set("eventhubs.name", name)
  }

  /** Set the key name for your EventHubs instance */
  def setKeyName(keyName: String): EventHubsConf = {
    set("eventhubs.keyName", keyName)
  }

  /** Set the key for your EventHubs instance */
  def setKey(key: String): EventHubsConf = {
    set("eventhubs.key", key)
  }

  /** Set the partition count of your EventHubs instance */
  def setPartitionCount(count: String): EventHubsConf = {
    set("eventhubs.partitionCount", count)
  }

  /** Set the progress directory used to checkpoint EventHubs specific files. */
  def setProgressDirectory(path: String): EventHubsConf = {
    set("eventhubs.progressDirectory", path)
  }

  /** Set the consumer group for your EventHubs instance. */
  def setConsumerGroup(consumerGroup: String): EventHubsConf = {
    set("eventhubs.consumerGroup", consumerGroup)
  }

  /** When set to true, all receivers will consume from the beginning of your EventHubs instance. */
  def setStartOfStream(b: Boolean): EventHubsConf = {
    if (b) setStartingWith("StartOfStream")
    set("eventhubs.startOfStream", b)
  }

  /** When set to true, all receivers will consume from the end of your EventHubs instance. */
  def setEndOfStream(b: Boolean): EventHubsConf = {
    if (b) setStartingWith("EndOfStream")
    set("eventhubs.endOfStream", b)
  }

  /** Set the max rate per partition. This will set all partitions with the same rate. */
  def setMaxRatePerPartition(rate: Int): EventHubsConf = {
    if (partitionCount.isEmpty) {
      logWarning("Your max rate was not set. The partition count must be set first.")
    } else {
      setMaxRatePerPartition(0 until partitionCount.get.toInt, rate)
    }
    this

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
    if (partitionCount.isEmpty) {
      logWarning("Your max rate was not set. The partition count must be set first.")
    } else if (!EventHubsConf.isSubset(partitionCount.get, range)) {
      logWarning(s"Your start offsets were not set. $range contains invalid partitions.")
    } else {
      val newRates: Map[PartitionId, Rate] =
        (for { partitionId <- range } yield partitionId -> rate).toMap
      _maxRatesPerPartition ++= newRates
    }
    this
  }

  /**
   * Set your starting offsets. Spark will consume from these offsets on startup.
   * This will set all partitions with the same starting offsets.
   */
  def setStartOffsets(offset: Offset): EventHubsConf = {
    if (partitionCount.isEmpty) {
      logWarning("Your start offsets were not set. The partition count must be set first.")
    } else {
      setStartOffsets(0 until partitionCount.get.toInt, offset)
    }
    this
  }

  /**
   * Set your starting offsets. This allows you to set starting offsets on a per partition basis.
   * If you don't specify an offset for a specific partition, we'll start from [[DefaultStartOffset]]
   *
   * Example:
   * {{{
   * // Set offset to 20 for partition 1.
   * ehConf.setStartOffsets(1 to 1, 20) // inclusive option
   * ehConf.setStartOffsets(1 until 2, 20) // exclusive option
   *
   * // Set rate to 50 for partition 4, 5, and 6.
   * ehConf.setStartOffsets(4 to 6, 50) // inclusive option
   * ehConf.setStartOffsets(4 until 7, 50) // exclusive option }}}
   */
  def setStartOffsets(range: Range, offset: Offset): EventHubsConf = {
    if (partitionCount.isEmpty) {
      logWarning("Your start offsets were not set. The partition count must be set first.")
    } else if (!EventHubsConf.isSubset(partitionCount.get, range)) {
      logWarning(s"Your start offsets were not set. $range contains invalid partitions.")
    } else {
      setStartingWith("Offsets")
      val newOffsets: Map[PartitionId, Offset] =
        (for { partitionId <- range } yield partitionId -> offset).toMap
      _startOffsets ++= newOffsets
    }
    this
  }

  /** Behavior is the same as [[setStartOffsets(Offset)]] (except with enqueue times!) */
  def setStartEnqueueTimes(enqueueTime: EnqueueTime): EventHubsConf = {
    if (partitionCount.isEmpty) {
      logWarning("Your enqueue times were not set. The partition count must be set first.")
    } else {
      setStartEnqueueTimes(0 until partitionCount.get.toInt, enqueueTime)
    }
    this
  }

  /** Behavior is the same as [[setStartOffsets(Range, Offset)]] (except with enqueue times!) */
  def setStartEnqueueTimes(range: Range, enqueueTime: EnqueueTime): EventHubsConf = {
    if (partitionCount.isEmpty) {
      logWarning("Your enqueue times were not set. The partition count must be set first.")
    } else if (!EventHubsConf.isSubset(partitionCount.get, range)) {
      logWarning(s"Your enqueue times were not set. $range contains invalid partitions.")
    } else {
      setStartingWith("EnqueueTimes")
      val newEnqueueTimes =
        (for { partitionId <- range } yield partitionId -> enqueueTime).toMap
      _startEnqueueTimes ++= newEnqueueTimes
    }
    this
  }

  /**
   * Set the receiver timeout. We will try to receive the expected batch for the length of this timeout.
   * Default: [[DefaultReceiverTimeout]]
   */
  def setReceiverTimeout(d: Duration): EventHubsConf = {
    set("eventhubs.receiverTimeout", d)
  }

  /**
   * Set the operation timeout. We will retry failures when contacting the EventHubs service for the length of this timeout.
   * Default: [[DefaultOperationTimeout]]
   */
  def setOperationTimeout(d: Duration): EventHubsConf = {
    set("eventhubs.operationTimeout", d)
  }

  /** Set to true if you want EventHubs properties to be included in your DataFrame.  */
  def setSqlContainsProperties(b: Boolean): EventHubsConf = {
    set("eventhubs.sql.containsProperties", b)
  }

  /** If your EventHubs data has user-defined keys, set them here.  */
  def setSqlUserDefinedKeys(keys: String*): EventHubsConf = {
    set("eventhubs.sql.userDefinedKeys", keys.toSet.mkString(","))
  }

  /** The currently set namespace. */
  def namespace: Option[String] = {
    self.get("eventhubs.namespace")
  }

  /** The currently set URI. */
  def uri: Option[String] = {
    self.namespace
  }

  /** The currently set EventHubs name. */
  def name: Option[String] = {
    self.get("eventhubs.name")
  }

  /** The currently set key name. */
  def keyName: Option[String] = {
    self.get("eventhubs.keyName")
  }

  /** The currently set key. */
  def key: Option[String] = {
    self.get("eventhubs.key")
  }

  /** The currently set partition count. */
  def partitionCount: Option[String] = {
    self.get("eventhubs.partitionCount")
  }

  /** The currently set progress directory. */
  def progressDirectory: Option[String] = {
    self.get("eventhubs.progressDirectory")
  }

  /** The currently set consumer group. */
  def consumerGroup: Option[String] = {
    self.get("eventhubs.consumerGroup")
  }

  /** The current value of startOfStream. */
  def startOfStream: Option[Boolean] = {
    self.get("eventhubs.startOfStream") map (str => str.toBoolean)
  }

  /** The current value of endOfStream */
  def endOfStream: Option[Boolean] = {
    self.get("eventhubs.endOfStream") map (str => str.toBoolean)
  }

  /** A map of partition/max rate pairs that have been set by the user.  */
  def maxRatesPerPartition: Map[PartitionId, Rate] = {
    _maxRatesPerPartition
  }

  /** A map of partition/offset pairs that have been set by the user. */
  def startOffsets: Map[PartitionId, Offset] = {
    _startOffsets
  }

  /** A map of partition/enqueue time pairs that have been set by the user. */
  def startEnqueueTimes: Map[PartitionId, EnqueueTime] = {
    _startEnqueueTimes
  }

  /** The current receiver timeout.  */
  def receiverTimeout: Option[Duration] = {
    self.get("eventhubs.receiverTimeout") map (str => Duration.parse(str))

  }

  /** The current operation timeout. */
  def operationTimeout: Option[Duration] = {
    self.get("eventhubs.operationTimeout") map (str => Duration.parse(str))
  }

  /** Whether EventHubsConf currently contains sql properties. */
  def sqlContainsProperties: Option[Boolean] = {
    self.get("eventhubs.sql.containsProperties") map (str => str.toBoolean)
  }

  /** Current user defined keys. */
  def sqlUserDefinedKeys: Option[Array[String]] = {
    self.get("eventhubs.sql.userDefinedKeys") map (str => str.split(","))
  }

  /** All max rates currently set will be erased. */
  def clearMaxRates(): EventHubsConf = {
    self._maxRatesPerPartition = Map.empty
    this
  }

  /** All start offsets currently set will be erased. */
  def clearStartOffsets(): EventHubsConf = {
    self._startOffsets = Map.empty
    this
  }

  /** All start enqueue times currently set will be erased. */
  def clearStartEnqueueTimes(): EventHubsConf = {
    self._startEnqueueTimes = Map.empty
    this
  }
}

object EventHubsConf extends Logging {

  /** Creates an EventHubsConf */
  def apply() = new EventHubsConf

  private[spark] def toConf(params: Map[String, String]): EventHubsConf = {
    val ehConf = EventHubsConf()
    for ((k, v) <- params) {
      ehConf.set(k, v)
    }

    ehConf.startingWith = params("eventhubs.startingWith")
    params("eventhubs.startingWith") match {
      case "EnqueueTimes" =>
        ehConf._startEnqueueTimes = parseEnqueueTimes(params("eventhubs.startEnqueueTimes"))
      case "Offsets" => ehConf._startOffsets = parseOffsets(params("eventhubs.startOffsets"))
      case _         =>
    }

    ehConf._maxRatesPerPartition = parseMaxRatesPerPartition(params("eventhubs.maxRates"))

    ehConf
  }

  private def isSubset(partitionCount: String, subset: Range): Boolean = {
    val maxRange = 0 until partitionCount.toInt
    subset.start >= maxRange.start && subset.end <= maxRange.end
  }

  private[common] def parseOffsets(startOffsets: String): Map[PartitionId, Offset] = {
    for { (k, v) <- stringToMap(startOffsets) } yield k -> v.toOffset
  }

  private[common] def parseEnqueueTimes(
      startEnqueueTimes: String): Map[PartitionId, EnqueueTime] = {
    for { (k, v) <- stringToMap(startEnqueueTimes) } yield k -> v.toEnqueueTime
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

  private[common] def offsetsToString(offsets: Map[PartitionId, Offset]): String = {
    mapToString(offsets)
  }

  private[common] def enqueueTimesToString(enqueueTimes: Map[PartitionId, EnqueueTime]): String = {
    mapToString(enqueueTimes)
  }

  private[common] def maxRatesToString(maxRates: Map[PartitionId, Rate]): String = {
    mapToString(maxRates)
  }

  private def mapToString(m: Map[PartitionId, _]): String = {
    val ordered = m.toSeq.sortBy(_._1)
    (for { (partition, value) <- ordered } yield s"$partition:$value").mkString(",")
  }
}
