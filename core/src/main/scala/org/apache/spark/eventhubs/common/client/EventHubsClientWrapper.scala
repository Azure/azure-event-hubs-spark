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
import java.time.{ Duration, Instant }
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentMap }

import scala.collection.JavaConverters._
import EventHubsOffsetTypes.EventHubsOffsetType
import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.common.EventHubsConf
import org.apache.spark.internal.Logging

import scala.util.{ Failure, Success, Try }

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClientWrapper(private val ehConf: EventHubsConf)
    extends Serializable
    with Client
    with Logging {

  import org.apache.spark.eventhubs.common._

  override private[spark] var client: EventHubClient = _
  private[spark] var partitionReceiver: PartitionReceiver = _

  /* Extract relevant info from ehParams */
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

  client = EventHubClient.createFromConnectionStringSync(connectionString.toString)

  private[spark] override def receiver(partitionId: String,
                                       seqNo: SequenceNumber): PartitionReceiver = {

    // TODO: just so this build. Remove this once API is actually available.
    implicit class SeqNoAPI(val client: EventHubClient) extends AnyVal {
      def createReceiver(consumerGroup: String,
                         partitionId: String,
                         seqNo: Long,
                         inclusiveSeqNo: Boolean): PartitionReceiver =
        null
    }

    if (partitionReceiver == null) {
      logInfo(s"Starting receiver for partitionId $partitionId from seqNo $seqNo")
      client.createReceiver(consumerGroup, partitionId, seqNo, true)
      partitionReceiver.setReceiveTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
    }
    partitionReceiver
  }

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfo(nameAndPartition: NameAndPartition) = {
    try {
      val partitionId = nameAndPartition.partitionId.toString
      client.getPartitionRuntimeInformation(partitionId).get
    } catch {
      case e: Exception =>
        e.printStackTrace()
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
      runtimeInformation.getBeginSequenceNumber
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  /**
   * return the end point of each partition
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    try {
      val runtimeInfo = getRunTimeInfo(NameAndPartition(ehName, partitionId))
      runtimeInfo.getLastEnqueuedSequenceNumber
    } catch {
      case e: Exception =>
        e.printStackTrace()
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
        e.printStackTrace()
        throw e
    }
  }

  override def partitionCount(): Int = {
    try {
      val runtimeInfo = client.getRuntimeInformation.get
      runtimeInfo.getPartitionCount
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  override def close(): Unit = {
    logInfo("close: Closing EventHubsClientWrapper.")
    if (partitionReceiver != null) partitionReceiver.closeSync()
    if (client != null) client.closeSync()
  }

  /**
   * Translate will take any starting point, and then return the corresponding Offset and SequenceNumber.
   */
  override def translate[T](ehConf: EventHubsConf): Map[PartitionId, SequenceNumber] = {
    val startingWith = ehConf("eventhubs.startingWith")
    val partitionCount = ehConf("eventhubs.partitionCount").toInt
    val result = new ConcurrentHashMap[PartitionId, SequenceNumber]()

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
                  .createReceiver(
                    DefaultConsumerGroup,
                    partitionId.toString,
                    Instant.ofEpochSecond(enqueueTimes.getOrElse(partitionId, DefaultEnqueueTime)))
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
            // TODO for now this is hardcoded. This needs to change to PartitionReceiver.PREFETCH_COUNT_MINIMUM.
            // It will be available next EH Client release.
            receiver.setPrefetchCount(10)
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
}

private[spark] object EventHubsClientWrapper {
  private[spark] def apply(ehConf: EventHubsConf): EventHubsClientWrapper =
    new EventHubsClientWrapper(ehConf)

  def userAgent: String = { EventHubClient.userAgent }

  def userAgent_=(str: String) { EventHubClient.userAgent = str }
}
