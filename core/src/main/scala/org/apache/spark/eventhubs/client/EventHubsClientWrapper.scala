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

package org.apache.spark.eventhubs.client

import java.util.concurrent.{ ConcurrentHashMap, Executors }

import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.internal.Logging
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.parallel.immutable.ParVector

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClientWrapper(private val ehConf: EventHubsConf)
    extends Serializable
    with Client
    with Logging {

  import org.apache.spark.eventhubs._

  private implicit val formats = Serialization.formats(NoTypeHints)

  private val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
  private val connectionString = ConnectionStringBuilder(ehConf.connectionString)
  connectionString.setOperationTimeout(ehConf.operationTimeout.getOrElse(DefaultOperationTimeout))

  private lazy val client: EventHubClient =
    EventHubClient.createFromConnectionStringSync(connectionString.toString,
                                                  Executors.newFixedThreadPool(1))

  private var receiver: PartitionReceiver = _
  def createReceiver(partitionId: String, startingSeqNo: SequenceNumber) = {
    if (receiver == null) {
      logInfo(s"Starting receiver for partitionId $partitionId from seqNo $startingSeqNo")
      receiver = client
        .createReceiver(consumerGroup,
                        partitionId,
                        EventPosition.fromSequenceNumber(startingSeqNo).convert)
        .get
      receiver.setReceiveTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
    }
  }

  override def setPrefetchCount(count: Int): Unit = {
    receiver.setPrefetchCount(count)
  }

  override def receive(eventCount: Int): java.lang.Iterable[EventData] = {
    require(receiver != null, "receive: PartitionReceiver has not been created.")
    receiver.receive(eventCount).get
  }

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfo(partitionId: PartitionId): EventHubPartitionRuntimeInformation = {
    try {
      client.getPartitionRuntimeInformation(partitionId.toString).get
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
  override def earliestSeqNo(partitionId: PartitionId): SequenceNumber = {
    try {
      val runtimeInformation = getRunTimeInfo(partitionId)
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
      val runtimeInfo = getRunTimeInfo(partitionId)
      runtimeInfo.getLastEnqueuedSequenceNumber + 1
    } catch {
      case e: Exception =>
        throw e
    }
  }

  private var _partitionCount = 0

  /**
   * The number of partitions in the EventHubs instance.
   *
   * @return partition count
   */
  override def partitionCount: Int = {
    if (_partitionCount == 0) {
      try {
        val runtimeInfo = client.getRuntimeInformation.get
        _partitionCount = runtimeInfo.getPartitionCount
      } catch {
        case e: Exception =>
          throw e
      }
    }
    _partitionCount
  }

  override def close(): Unit = {
    logInfo("close: Closing EventHubsClientWrapper.")
    if (receiver != null) receiver.closeSync()
    if (client != null) client.closeSync()
  }

  /**
   * Convert any starting positions to the corresponding sequence number.
   */
  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int,
                            useStart: Boolean = true): Map[PartitionId, SequenceNumber] = {

    val result = new ConcurrentHashMap[PartitionId, SequenceNumber]()
    val needsTranslation = ParVector[Int]()

    val positions = if (useStart) {
      ehConf.startingPositions.getOrElse(Map.empty).par
    } else {
      ehConf.endingPositions.getOrElse(Map.empty).par
    }

    val defaultPos = if (useStart) {
      ehConf.startingPosition.getOrElse(DefaultEventPosition)
    } else {
      ehConf.endingPosition.getOrElse(DefaultEndingPosition)
    }

    // Partitions which have a sequence number position are put in result.
    // All other partitions need to be translated into sequence numbers by the service.
    (0 until partitionCount).par.foreach { id =>
      val position = positions.getOrElse(id, defaultPos)
      if (position.seqNo >= 0L) {
        result.put(id, position.seqNo)
      } else {
        needsTranslation :+ id
      }
    }

    val threads = ParVector[Thread]()
    needsTranslation.foreach(partitionId => {
      threads :+ new Thread {
        override def run(): Unit = {
          val receiver = client
            .createReceiver(DefaultConsumerGroup,
                            partitionId.toString,
                            positions.getOrElse(partitionId, defaultPos).convert)
            .get
          receiver.setPrefetchCount(PrefetchCountMinimum)
          val event = receiver.receive(1).get.iterator().next() // get the first event that was received.
          receiver.close().get()
          result.put(partitionId, event.getSystemProperties.getSequenceNumber)
        }
      }
    })

    logInfo("translate: Starting threads to translate to sequence number.")
    threads.foreach(_.start())
    threads.foreach(_.join())
    logInfo("translate: Translation complete.")

    require(partitionCount == result.size)

    result.asScala.toMap.mapValues { seqNo =>
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
