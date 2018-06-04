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

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import com.microsoft.azure.eventhubs._
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.internal.Logging
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{ Failure, Success, Try }

/**
 * A [[Client]] which connects to an event hub instance. All interaction
 * between Spark and Event Hubs will happen within this client.
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClient(private val ehConf: EventHubsConf)
    extends Serializable
    with Client
    with Logging {

  import org.apache.spark.eventhubs._

  ehConf.validate

  private implicit val formats = Serialization.formats(NoTypeHints)

  private var _client: EventHubClient = _
  private def client = synchronized {
    if (_client == null) {
      _client = ClientConnectionPool.borrowClient(ehConf)
    }
    _client
  }

  // TODO support multiple partitions senders
  private var partitionSender: PartitionSender = _
  override def createPartitionSender(partition: Int): Unit = {
    val id = partition.toString
    if (partitionSender == null) {
      logInfo(s"Creating partition sender for $partition for EventHub ${client.getEventHubName}")
      partitionSender = client.createPartitionSenderSync(id)
    } else if (partitionSender.getPartitionId != id) {
      logInfo(
        s"Closing partition sender for ${partitionSender.getPartitionId} for EventHub ${client.getEventHubName}")
      partitionSender.closeSync()
      logInfo(s"Creating partition sender for $partition for EventHub ${client.getEventHubName}")
      partitionSender = client.createPartitionSenderSync(id)
    }
  }

  override def send(event: EventData,
                    partition: Option[Rate] = None,
                    partitionKey: Option[String] = None): Unit = {
    if (partition.isDefined) {
      require(partitionSender.getPartitionId.toInt == partition.get)
      partitionSender.sendSync(event)
    } else if (partitionKey.isDefined) {
      client.sendSync(event, partitionKey.get)
    } else {
      client.sendSync(event)
    }
  }

  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case Failure(e: EventHubException) if e.getIsTransient && n > 1 =>
        logInfo("Retrying getRunTimeInfo failure.", e)
        retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }

  private def getRunTimeInfo(partitionId: PartitionId): PartitionRuntimeInformation = {
    retry(RetryCount) {
      client.getPartitionRuntimeInformation(partitionId.toString).get
    }
  }

  /**
   * Provides the earliest (lowest) sequence number that exists in the
   * EventHubs instance for the given partition.
   *
   * @param partition the partition that will be queried
   * @return the earliest sequence number for the specified partition
   */
  override def earliestSeqNo(partition: PartitionId): SequenceNumber = {
    try {
      val runtimeInformation = getRunTimeInfo(partition)
      val seqNo = runtimeInformation.getBeginSequenceNumber
      if (seqNo == -1L) 0L else seqNo
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Provides the latest (highest) sequence number that exists in the EventHubs
   * instance for the given partition.
   *
   * @param partition the partition that will be queried
   * @return the latest sequence number for the specified partition
   */
  override def latestSeqNo(partition: PartitionId): SequenceNumber = {
    try {
      val runtimeInfo = getRunTimeInfo(partition)
      runtimeInfo.getLastEnqueuedSequenceNumber + 1
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Provides the earliest and the latest sequence numbers in the provided
   * partition.
   *
   * @param partition the partition that will be queried
   * @return the earliest and latest sequence numbers for the specified partition.
   */
  override def boundedSeqNos(partition: PartitionId): (SequenceNumber, SequenceNumber) = {
    try {
      val runtimeInfo = getRunTimeInfo(partition)
      val earliest =
        if (runtimeInfo.getBeginSequenceNumber == -1L) 0L else runtimeInfo.getBeginSequenceNumber
      val latest = runtimeInfo.getLastEnqueuedSequenceNumber + 1
      (earliest, latest)
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * The number of partitions in the EventHubs instance.
   *
   * @return partition count
   */
  override lazy val partitionCount: Int = {
    try {
      val runtimeInfo = client.getRuntimeInformation.get
      runtimeInfo.getPartitionCount
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Cleans up all open connections and links.
   *
   * The [[EventHubClient]] is returned to the [[ClientConnectionPool]].
   *
   * [[PartitionSender]] is closed.
   */
  override def close(): Unit = {
    logInfo("close: Closing EventHubsClient.")
    if (partitionSender != null) {
      partitionSender.closeSync()
      partitionSender = null
    }
    if (_client != null) {
      ClientConnectionPool.returnClient(ehConf, _client)
      _client = null
    }
  }

  /**
   * Translates all [[EventPosition]]s provided in the [[EventHubsConf]] to
   * sequence numbers. Sequence numbers are zero-based indices. The 5th event
   * in an Event Hubs partition will have a sequence number of 4.
   *
   * This allows us to exclusively use sequence numbers to generate and manage
   * batches within Spark (rather than coding for many different filter types).
   *
   * @param ehConf the [[EventHubsConf]] containing starting (or ending positions)
   * @param partitionCount the number of partitions in the Event Hub instance
   * @param useStart translates starting positions when true and ending positions
   *                 when false
   */
  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int,
                            useStart: Boolean = true): Map[PartitionId, SequenceNumber] = {
    val result = new ConcurrentHashMap[PartitionId, SequenceNumber]()
    val needsTranslation = ArrayBuffer[NameAndPartition]()

    logInfo(s"translate: useStart is set to $useStart.")
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
    logInfo(s"translate: PerPartitionPositions = $positions")
    logInfo(s"translate: Default position = $defaultPos")

    // Partitions which have a sequence number position are put in result.
    // All other partitions need to be translated into sequence numbers by the service.
    (0 until partitionCount).par.foreach { id =>
      val nAndP = NameAndPartition(ehConf.name, id)
      val position = positions.getOrElse(nAndP, defaultPos)
      if (position.seqNo >= 0L) {
        result.put(id, position.seqNo)
      } else {
        synchronized(needsTranslation += nAndP)
      }
    }
    logInfo(s"translate: needsTranslation = $needsTranslation")

    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val threads = ArrayBuffer[Thread]()
    needsTranslation.foreach(nAndP => {
      val partitionId = nAndP.partitionId
      threads += new Thread {
        override def run(): Unit = {
          @volatile var receiver: PartitionReceiver = null
          try {
            receiver = client
              .createReceiverSync(consumerGroup,
                                  partitionId.toString,
                                  positions.getOrElse(nAndP, defaultPos).convert)
            receiver.setPrefetchCount(PrefetchCountMinimum)
            receiver.setReceiveTimeout(Duration.ofSeconds(5))
            val events = receiver.receiveSync(1) // get the first event that was received.
            if (events == null || !events.iterator.hasNext) {
              logWarning(
                "translate: failed to translate event. There are three cases in which we fail" +
                  "to translate events: 1) We are receiving from the EndOfStream and no new " +
                  "events are being sent, 2) The partition is empty, or 3) The user passed" +
                  "an invalid offset. In any case, when a failure occurs, we will start from" +
                  "the end of the stream (e.g. the latest events in your partition). ")
              val (earliest, latest) = boundedSeqNos(partitionId)
              if (earliest >= latest) {
                result.put(partitionId, earliest)
              } else {
                result.put(partitionId, latest)
              }
            } else {
              val event = events.iterator.next
              result.put(partitionId, event.getSystemProperties.getSequenceNumber)
            }
          } catch {
            case e: IllegalEntityException =>
              logError("translate: IllegalEntityException. Consumer group may not exist.", e)
              throw e
          } finally {
            if (receiver != null) {
              receiver.closeSync()
            }
          }
        }
      }
    })

    logInfo("translate: Starting threads to translate to sequence number.")
    threads.foreach(_.start())
    threads.foreach(_.join())
    logInfo("translate: Translation complete.")
    logInfo(s"translate: result = $result")

    assert(result.size == partitionCount,
           s"translate: result size ${result.size} does not equal partition count $partitionCount")

    result.asScala.toMap
      .mapValues { seqNo =>
        { if (seqNo == -1L) 0L else seqNo }
      }
      .map(identity)
  }
}

private[spark] object EventHubsClient {
  private[spark] def apply(ehConf: EventHubsConf): EventHubsClient =
    new EventHubsClient(ehConf)

  /**
   * @return the currently set user agent
   */
  def userAgent: String = {
    EventHubClientImpl.USER_AGENT
  }

  /**
   * A user agent is set whenever the connector is initialized.
   * In the case of the connector, the user agent is the Spark
   * version in use.
   *
   * @param user_agent the user agent
   */
  def userAgent_=(user_agent: String) {
    EventHubClientImpl.USER_AGENT = user_agent
  }
}
