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

// TODO I think Structured Streaming needs this convert to a map.
// So I need to write an implicit converter for Structured Streaming.
// And need to tackle how to encode the per partition stuff. Might not be able to.
// TODO make Direct Streams just use EventHubsConf directly.

package org.apache.spark.eventhubs.common

import java.net.URI
import java.time.Duration

import org.apache.spark.internal.Logging

import language.implicitConversions

// TODO maybe it'd be best to make nothing required in the constructor. it's more legible that way.
// TODO double check the ranges - make sure they're inclusive.
final class EventHubsConf private (
    private var _name: String,
    private var _keyName: String,
    private var _key: String,
    private var _partitionCount: String,
    private var _progressDir: String
) extends Serializable
    with Logging {

  def this(namespace: String,
           name: String,
           keyName: String,
           key: String,
           partitionCount: String,
           progressDir: String) {
    this(name, keyName, key, partitionCount, progressDir)
    _namespace = namespace
  }

  def this(uri: URI,
           name: String,
           keyName: String,
           key: String,
           partitionCount: String,
           progressDir: String) {
    this(name, keyName, key, partitionCount, progressDir)
    _uri = uri
  }

  private var _uri: URI = _
  def uri: URI = _uri
  def setURI(u: URI): EventHubsConf = {
    _uri = u
    this
  }

  private var _namespace: String = _
  def namespace: String = _namespace
  def setNamespace(ns: String): EventHubsConf = {
    _namespace = ns
    this
  }

  def name: String = _name
  def setName(n: String): EventHubsConf = {
    _name = n
    this
  }

  def keyName: String = _keyName
  def setKeyName(kn: String): EventHubsConf = {
    _keyName = kn
    this
  }

  def key: String = _key
  def setKey(k: String): EventHubsConf = {
    _key = k
    this
  }

  private var _consumerGroup: String = EventHubsUtils.DefaultConsumerGroup
  def consumerGroup: String = _consumerGroup
  def setConsumerGroup(cg: String): EventHubsConf = {
    _consumerGroup = cg
    this
  }

  def partitionCount: String = _partitionCount
  def setPartitionCount(pc: String): EventHubsConf = {
    _partitionCount = pc
    this
  }

  def progressDir: String = _progressDir
  def setProgressDir(pd: String): EventHubsConf = {
    _progressDir = pd
    this
  }

  private var _maxRatePerPartition: Map[PartitionId, Rate] = (for {
    partitionId <- 0 until partitionCount.toInt
  } yield partitionId -> EventHubsUtils.DefaultMaxRatePerPartition).toMap
  def maxRatePerPartition: Map[PartitionId, Rate] = _maxRatePerPartition
  def setMaxRatePerPartition(rate: Int): EventHubsConf = {
    setMaxRatePerPartition(0 until partitionCount.toInt, rate)
  }
  def setMaxRatePerPartition(range: Range, rate: Rate): EventHubsConf = {
    val newRates: Map[PartitionId, Rate] = (for {
      partitionId <- range
    } yield partitionId -> rate).toMap
    _maxRatePerPartition ++= newRates
    this
  }

  private var _startOffsets: Map[PartitionId, Offset] = _
  def startOffsets: Map[PartitionId, Offset] = _startOffsets
  def setStartOffsets(offset: Offset): EventHubsConf = {
    setStartOffsets(0 until partitionCount.toInt, offset)
  }
  def setStartOffsets(range: Range, offset: Offset): EventHubsConf = {
    val newOffsets: Map[PartitionId, Offset] = (for {
      partitionId <- range
    } yield partitionId -> offset).toMap
    _startOffsets ++= newOffsets
    this
  }

  private var _startEnqueueTimes: Map[PartitionId, EnqueueTime] = _
  def startEnqueueTimes: Map[PartitionId, EnqueueTime] = _startEnqueueTimes
  def setStartEnqueueTimes(enqueueTime: EnqueueTime): EventHubsConf = {
    setStartEnqueueTimes(0 until partitionCount.toInt, enqueueTime)
  }
  def setStartEnqueueTimes(range: Range, enqueueTime: EnqueueTime): EventHubsConf = {
    val newEnqueueTimes = (for {
      partitionId <- range
    } yield partitionId -> enqueueTime).toMap
    _startEnqueueTimes ++= newEnqueueTimes
    this
  }

  private var _startOfStream: Boolean = false
  def startOfStream: Boolean = _startOfStream
  def setStartOfStream(b: Boolean): EventHubsConf = {
    _startOfStream = b
    this
  }

  private var _endOfStream: Boolean = false
  def endOfStream: Boolean = _endOfStream
  def setEndOfStream(b: Boolean): EventHubsConf = {
    _endOfStream = b
    this
  }

  private var _receiverTimeout: Duration = EventHubsUtils.DefaultReceiverTimeout
  def receiverTimeout: Duration = _receiverTimeout
  def setReceiverTimeout(d: Duration): EventHubsConf = {
    _receiverTimeout = d
    this
  }

  private var _operationTimeout: Duration = EventHubsUtils.DefaultOperationTimeout
  def operationTimeout: Duration = _operationTimeout
  def setOperationTimeout(d: Duration): EventHubsConf = {
    _operationTimeout = d
    this
  }

  // TODO: are there any other SQL ones??????
  private var _sqlContainsProperties: Boolean = false
  def sqlContainsProperties: Boolean = _sqlContainsProperties
  def setSqlContainsProperties(b: Boolean): EventHubsConf = {
    _sqlContainsProperties = b
    this
  }

  private var _sqlUserDefinedKeys: Set[String] = Set()
  def sqlUserDefinedKeys: Set[String] = _sqlUserDefinedKeys
  def setSqlUserDefinedKeys(keys: String*): EventHubsConf = {
    _sqlUserDefinedKeys ++= keys.toSet
    this
  }
}

object EventHubsConf extends Logging {
  def validate(ehConf: EventHubsConf): Boolean = {
    if (ehConf.uri != null)
      require(ehConf.namespace == null, "If a URI is provided, you cannot provide a namespace.")
    else
      require(ehConf.namespace != null, "Either a namespace or a URI must be provided.")

    if (ehConf.startOfStream) {
      require(!ehConf.endOfStream,
              "validate: Both startOfStream and endOfStream cannot be set to true.")
      require(
        ehConf.startOffsets == null,
        "validate: startOfStream is true and startOffsets have been set. This is not allowed.")
      require(
        ehConf.startEnqueueTimes == null,
        "validate: startOfStream is true and startEnqueueTimes have been set. This is not allowed.")
      logInfo(
        "validate: startOfStream is set to true. All partitions will start from the beginning of the stream.")
    } else if (ehConf.endOfStream) {
      require(ehConf.startOffsets == null,
              "validate: endOfStream is true and startOffsets have been set. This is not allowed.")
      require(
        ehConf.startEnqueueTimes == null,
        "validate: endOfStream is true and startEnqueueTimes have been set. This is not allowed.")
      logInfo(
        "validate: endOfStream is set to true. All partitions will start from the beginning of the stream.")
    } else if (ehConf.startOffsets != null) {
      require(ehConf.startEnqueueTimes == null,
              "validate: startOffsets and startEnqueueTimes have been set. This is not allowed.")
      logInfo(
        s"validate: startOffsets ${ehConf.startOffsets} will be used as starting points for your stream.")
    } else if (ehConf.startEnqueueTimes != null) {
      logInfo(
        s"validate: startEnqueueTimes ${ehConf.startEnqueueTimes} will be used as starting points for your stream.")
    } else {
      logInfo(
        s"validate: no starting point set by user. Default to the start of your EventHubs stream.")
    }
    true
  }

  // TODO we have to explicitly convert this for structured streaming
  private def mapToConf(ehParams: Map[String, String]): EventHubsConf = {
    val uri = ehParams.getOrElse("eventhubs.uri", null)
    val namespace = ehParams.getOrElse("eventhubs.namespace", null)
    val name = ehParams("eventhubs.name")
    val policyname = ehParams("eventhubs.policyname")
    val policykey = ehParams("eventhubs.policykey")
    val partitionCount = ehParams("eventhubs.partition.count")
    val progressDir = ehParams("eventhubs.progressTrackingDir")
    val consumerGroup = ehParams.getOrElse("eventhubs.consumergroup", "$Default")
    val maxRates =
      ehParams.getOrElse("eventhubs.maxRate", null)
    val containsProperties = ehParams.getOrElse("eventhubs.sql.containsProperties", "false")
    val userDefinedKeys = ehParams.getOrElse("eventhubs.sql.userDefinedKeys", "")
    val receiverTimeout = ehParams.getOrElse("eventhubs.receiver.timeout", "5")
    val operationTimeout = ehParams.getOrElse("eventhubs.operation.timeout", "60")
    val startOfStream = ehParams.getOrElse("eventhubs.start.of.stream", "false")
    val endOfStream = ehParams.getOrElse("eventhubs.end.of.stream", "false")
    val startOffsets = ehParams.getOrElse("eventhubs.filter.offset", null)
    val startEnqueueTimes = ehParams.getOrElse("eventhubs.filter.enqueuetime", null)

    val ehConf = if (uri == null) {
      new EventHubsConf(namespace, name, policyname, policykey, partitionCount, progressDir)
    } else {
      new EventHubsConf(uri, name, policyname, policykey, partitionCount, progressDir)
    }

    ehConf.setConsumerGroup(consumerGroup)
    if (containsProperties.toBoolean)
      ehConf.setSqlContainsProperties(true)
    if (startOfStream.toBoolean)
      ehConf.setStartOfStream(true)
    if (endOfStream.toBoolean)
      ehConf.setEndOfStream(true)
    if (receiverTimeout != "5")
      ehConf.setReceiverTimeout(Duration.ofSeconds(receiverTimeout.toInt))
    if (operationTimeout != "60")
      ehConf.setOperationTimeout(Duration.ofSeconds(operationTimeout.toInt))

    if (userDefinedKeys != "") {
      val keys = userDefinedKeys.split(",").toSet
      for (key <- keys)
        ehConf.setSqlUserDefinedKeys(key)
    }

    if (maxRates != null) {
      val ratesAndPartitions = maxRates.split(",")
      for (rateAndPartition <- ratesAndPartitions) {
        rateAndPartition.split(":") match {
          case Array(partitionId, rate) =>
            ehConf.setMaxRatePerPartition(partitionId.toInt to partitionId.toInt, rate.toInt)
          case _ =>
            throw new IllegalStateException("mapToConf: Max rates are not formatted correctly.")
        }
      }
    }

    if (startEnqueueTimes != null) {
      val timesAndPartitions = startEnqueueTimes.split(",")
      for (timeAndPartition <- timesAndPartitions) {
        timeAndPartition.split(":") match {
          case Array(partitionId, time) =>
            ehConf.setStartEnqueueTimes(partitionId.toInt to partitionId.toInt, time.toLong)
          case _ =>
            throw new IllegalStateException(
              "mapToConf: Start enqueue times are not formatted correctly.")
        }
      }
    }

    if (startOffsets != null) {
      val offsetsAndPartitions = startOffsets.split(",")
      for (offsetAndPartition <- offsetsAndPartitions) {
        offsetAndPartition.split(":") match {
          case Array(partitionId, offset) =>
            ehConf.setStartOffsets(partitionId.toInt to partitionId.toInt, offset.toLong)
          case _ =>
            throw new IllegalStateException("mapToConf: Start offsets are not formatted correctly.")
        }
      }
    }

    ehConf
  }

  implicit def confToMap(ehConf: EventHubsConf): Map[String, String] = {
    var ehParams: Map[String, String] = Map()

    val uri: String = ehConf.uri.toString
    val namespace = ehConf.namespace
    val name = ehConf.name
    val policyname = ehConf.keyName
    val policykey = ehConf.key
    val partitionCount = ehConf.partitionCount
    val progressDir = ehConf.progressDir
    val consumerGroup = ehConf.consumerGroup
    val maxRates = ehConf.maxRatePerPartition
    val containsProperties = ehConf.sqlContainsProperties
    val userDefinedKeys = ehConf.sqlUserDefinedKeys
    val receiverTimeout = ehConf.receiverTimeout
    val operationTimeout = ehConf.operationTimeout
    val startOfStream = ehConf.startOfStream
    val endOfStream = ehConf.endOfStream
    val startOffsets = ehConf.startOffsets
    val startEnqueueTimes = ehConf.startEnqueueTimes

    val = plz don't build
    /*
        Putting a pin in all this for now. For when I come back to this. confToMap is implicit and public so that users don't see the conversion.
        mapToConf is NOT implicit and NOT public. users shouldn't need it. and i don't want any implicit conversions happening with this.

        you need to finish implementing confToMap and then write tests for EventHubsConf. Then get the rest of these tests to pass.
     */

    ehParams ++= Map("eventhubs.uri" -> uri)
    ehParams ++= Map("eventhubs.namespace" -> namespace)
    ehParams ++= Map("eventhubs.name" -> name)
    ehParams ++= Map("eventhubs.policyname" -> policyname)
    ehParams ++= Map("eventhubs.policykey" -> policykey)
    ehParams ++= Map("eventhubs.partition.count" -> partitionCount)
    ehParams ++= Map("eventhubs.progressTrackingDir" -> progressDir)

    val consumerGroup = ehParams.getOrElse("eventhubs.consumergroup", "$Default")
    val maxRates =
      ehParams.getOrElse("eventhubs.maxRate", null)
    val containsProperties = ehParams.getOrElse("eventhubs.sql.containsProperties", "false")
    val userDefinedKeys = ehParams.getOrElse("eventhubs.sql.userDefinedKeys", "")
    val receiverTimeout = ehParams.getOrElse("eventhubs.receiver.timeout", "5")
    val operationTimeout = ehParams.getOrElse("eventhubs.operation.timeout", "60")
    val startOfStream = ehParams.getOrElse("eventhubs.start.of.stream", "false")
    val endOfStream = ehParams.getOrElse("eventhubs.end.of.stream", "false")
    val startOffsets = ehParams.getOrElse("eventhubs.filter.offset", null)
    val startEnqueueTimes = ehParams.getOrElse("eventhubs.filter.enqueuetime", null)

  }
}
