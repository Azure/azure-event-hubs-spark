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

package org.apache.spark.eventhubs.common.client

import java.net.URI
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.common.EventHubsConf
import org.apache.spark.internal.Logging

import scala.util.Try

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClientWrapper(private val ehConf: EventHubsConf)
    extends Serializable
    with Client
    with Logging {

  import org.apache.spark.eventhubs.common._

  // Extract relevant info from ehParams
  private val ehNamespace = ehConf.namespace.get
  private val ehName = ehConf.name.get
  private val ehPolicyName = ehConf.keyName.get
  private val ehPolicy = ehConf.key.get
  private val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)

  private val connectionString =
    Try {
      new ConnectionStringBuilder(ehNamespace, ehName, ehPolicyName, ehPolicy)
    } getOrElse Try {
      new ConnectionStringBuilder(new URI(ehNamespace), ehName, ehPolicyName, ehPolicy)
    }.get
  connectionString.setOperationTimeout(ehConf.operationTimeout.getOrElse(DefaultOperationTimeout))

  private var client: EventHubClient =
    EventHubClient.createFromConnectionStringSync(connectionString.toString)

  private var receiver: PartitionReceiver = _
  private[spark] def createReceiver(partitionId: String, startingSeqNo: SequenceNumber) = {
    if (receiver == null) {
      logInfo(s"Starting receiver for partitionId $partitionId from seqNo $startingSeqNo")
      receiver = client.createReceiver(consumerGroup, partitionId, startingSeqNo, true)
      receiver.setReceiveTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
    }
  }

  override private[spark] def setPrefetchCount(count: Int): Unit = {
    receiver.setPrefetchCount(count)
  }

  override private[spark] def receive(eventCount: Int): java.lang.Iterable[EventData] = {
    require(receiver != null, "receive: PartitionReceiver has not been created.")
    receiver.receive(eventCount).get
  }

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfo(nameAndPartition: NameAndPartition) = {
    try {
      val partitionId = nameAndPartition.partitionId.toString
      client.getPartitionRuntimeInformation(partitionId).get
    } catch {
      case e: Exception =>
        throw e
    }
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def earliestSeqNo(nameAndPartition: NameAndPartition): SequenceNumber = {
    try {
      val runtimeInformation = getRunTimeInfo(nameAndPartition)
      val seqNo = runtimeInformation.getBeginSequenceNumber
      if (seqNo == -1L) 0L else seqNo
    } catch {
      case e: Exception =>
        throw e
    }
  }

  /**
   * Returns the end point of each partition
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    try {
      val runtimeInfo = getRunTimeInfo(NameAndPartition(ehName, partitionId))
      runtimeInfo.getLastEnqueuedSequenceNumber + 1
    } catch {
      case e: Exception =>
        throw e
    }
  }

  /**
   * return the last enqueueTime of each partition
   *
   * @return a map from eventHubsNamePartition to EnqueueTime
   */
  override def lastEnqueuedTime(nameAndPartition: NameAndPartition): EnqueueTime = {
    try {
      val runtimeInfo = getRunTimeInfo(nameAndPartition)
      runtimeInfo.getLastEnqueuedTimeUtc.getEpochSecond
    } catch {
      case e: Exception =>
        throw e
    }
  }

  override def partitionCount: Int = {
    try {
      val runtimeInfo = client.getRuntimeInformation.get
      runtimeInfo.getPartitionCount
    } catch {
      case e: Exception =>
        throw e
    }
  }

  override def close(): Unit = {
    logInfo("close: Closing EventHubsClientWrapper.")
    if (receiver != null) receiver.closeSync()
    if (client != null) client.closeSync()
  }

  /**
   * Translate will take any starting point, and then return the corresponding Offset and SequenceNumber.
   */
  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int): Map[PartitionId, SequenceNumber] = {
    val startingWith = ehConf("eventhubs.startingWith")
    val result = new ConcurrentHashMap[PartitionId, SequenceNumber]()

    val seqNos = if (startingWith equals "SequenceNumbers") {
      (for (partitionId <- 0 until partitionCount)
        yield
          partitionId -> ehConf.startSequenceNumbers.getOrElse(partitionId,
                                                               DefaultStartSequenceNumber)).toMap
    } else {
      val threads = for (partitionId <- 0 until partitionCount)
        yield
          new Thread {
            override def run(): Unit = {
              // Pattern  match is to create the correct receiver based on what we're startingWith.
              val receiver = startingWith match {
                case "Offsets" =>
                  val offsets = ehConf.startOffsets
                  client
                    .createReceiver(DefaultConsumerGroup,
                                    partitionId.toString,
                                    offsets.getOrElse(partitionId, DefaultStartOffset).toString,
                                    true)
                    .get
                case "EnqueueTimes" =>
                  val enqueueTimes = ehConf.startEnqueueTimes
                  client
                    .createReceiver(DefaultConsumerGroup,
                                    partitionId.toString,
                                    Instant.ofEpochSecond(
                                      enqueueTimes.getOrElse(partitionId, DefaultEnqueueTime)))
                    .get
                case "StartOfStream" =>
                  client
                    .createReceiver(DefaultConsumerGroup, partitionId.toString, StartOfStream, true)
                    .get
                case "EndOfStream" =>
                  client
                    .createReceiver(DefaultConsumerGroup, partitionId.toString, EndOfStream, true)
                    .get
              }
              receiver.setPrefetchCount(PrefetchCountMinimum)
              val event = receiver.receive(1).get.iterator().next() // get the first event that was received.
              receiver.close().get()
              result.put(partitionId, event.getSystemProperties.getSequenceNumber)
            }
          }

      logInfo("translate: Starting threads to translate to (offset, sequence number) pairs.")
      threads.foreach(thread => thread.start())
      threads.foreach(thread => thread.join())
      logInfo("translate: Translation complete.")
      result.asScala.toMap
    }
    seqNos.mapValues { seqNo =>
      { if (seqNo == -1L) 0L else seqNo }
    }
  }
}

private[spark] object EventHubsClientWrapper {
  private[spark] def apply(ehConf: EventHubsConf): EventHubsClientWrapper =
    new EventHubsClientWrapper(ehConf)

  def userAgent: String = { EventHubClient.userAgent }

  def userAgent_=(str: String) { EventHubClient.userAgent = str }
}
