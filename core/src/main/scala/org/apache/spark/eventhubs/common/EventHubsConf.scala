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

import java.time.Duration

import org.apache.spark.internal.Logging

import language.implicitConversions

final class EventHubsConf(
    private var _namespace: String,
    private var _name: String,
    private var _keyName: String,
    private var _key: String,
    private var _consumerGroup: String,
    private var _partitionCount: String,
    private var _progressDir: String
) extends Serializable
    with Logging {

  def namespace: String = _namespace
  def namespace(ns: String): Unit = {
    _namespace = ns
  }

  def name: String = _name
  def name(n: String): Unit = {
    _name = name
  }

  def keyName: String = _keyName
  def keyName(kn: String): Unit = {
    _keyName = kn
  }

  def key: String = _key
  def key(k: String): Unit = {
    _key = k
  }

  def consumerGroup: String = _consumerGroup
  def consumerGroup(cg: String): Unit = {
    _consumerGroup = cg
  }

  def partitionCount: String = _partitionCount
  def partitionCount(pc: String): Unit = {
    _partitionCount = pc
  }

  def progressDir: String = _progressDir
  def progressDir(pd: String): Unit = {
    _progressDir = pd
  }

  private var _maxRatePerPartition: Map[PartitionId, Rate] = (for {
    partitionId <- 0 to partitionCount.toInt
  } yield partitionId -> EventHubsUtils.DefaultMaxRatePerPartition).toMap
  def maxRatePerPartition: Map[PartitionId, Rate] = _maxRatePerPartition
  def maxRatePerPartition(rate: Int): Unit = {
    maxRatePerPartition(0 to partitionCount.toInt, rate)
  }
  def maxRatePerPartition(inclusive: Range.Inclusive, rate: Int): Unit = {
    val newRates: Map[PartitionId, Rate] = (for {
      partitionId <- inclusive
    } yield partitionId -> rate).toMap
    _maxRatePerPartition ++= newRates
  }

  private var _startOffsets: Map[PartitionId, Offset] = _
  def startOffsets: Map[PartitionId, Offset] = _startOffsets
  def startOffsets(offset: Offset): Unit = {
    startOffsets(0 to partitionCount.toInt, offset)
  }
  def startOffsets(inclusive: Range.Inclusive, offset: Offset): Unit = {
    val newOffsets: Map[PartitionId, Offset] = (for {
      partitionId <- inclusive
    } yield partitionId -> offset).toMap
    _startOffsets ++= newOffsets
  }

  private var _startEnqueueTimes: Map[PartitionId, EnqueueTime] = _
  def startEnqueueTimes: Map[PartitionId, EnqueueTime] = _startEnqueueTimes
  def startEnqueueTimes(enqueueTime: EnqueueTime): Unit = {
    startEnqueueTimes(0 to partitionCount.toInt, enqueueTime)
  }
  def startEnqueueTimes(inclusive: Range.Inclusive, enqueueTime: EnqueueTime): Unit = {
    val newEnqueueTimes = (for {
      partitionId <- inclusive
    } yield partitionId -> enqueueTime).toMap
    _startEnqueueTimes ++= newEnqueueTimes
  }

  private var _startOfStream: Boolean = false
  def startOfStream: Boolean = _startOfStream
  def startOfStream(b: Boolean): Unit = {
    _startOfStream = b
  }

  private var _endOfStream: Boolean = false
  def endOfStream: Boolean = _endOfStream
  def endOfStream(b: Boolean): Unit = {
    _endOfStream = b
  }

  private var _receiverTimeout: Duration = EventHubsUtils.DefaultReceiverTimeout
  def receiverTimeout: Duration = _receiverTimeout
  def receiverTimeout(d: Duration): Unit = {
    _receiverTimeout = d
  }

  private var _operationTimeout: Duration = EventHubsUtils.DefaultOperationTimeout
  def operationTimeout: Duration = _operationTimeout
  def operationTimeout(d: Duration): Unit = {
    _operationTimeout = d
  }

  // TODO: are there any other SQL ones??????
  private var _sqlContainsProperties: Boolean = false
  def sqlContainsProperties: Boolean = _sqlContainsProperties
  def sqlContainsProperties(b: Boolean): Unit = {
    _sqlContainsProperties = b
  }

  private var _sqlUserDefinedKeys: Set[String] = Set()
  def sqlUserDefinedKeys: Set[String] = _sqlUserDefinedKeys
  def sqlUserDefinedKeys(keys: String*): Unit = {
    _sqlUserDefinedKeys ++= keys.toSet
  }
}

object EventHubsConf extends Logging {
  def validate(ehConf: EventHubsConf): Boolean = {
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
}
