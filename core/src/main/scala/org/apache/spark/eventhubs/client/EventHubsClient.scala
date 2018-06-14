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

import java.util.concurrent.CompletableFuture

import com.microsoft.azure.eventhubs._
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.internal.Logging
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.compat.java8.FutureConverters._
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

  private def getRunTimeInfoF(partitionId: PartitionId): Future[PartitionRuntimeInformation] = {
    retry(RetryCount) {
      toScala(client.getPartitionRuntimeInformation(partitionId.toString))
    }
  }

  /*
  private def boundedSeqNosF(partition: PartitionId): CompletableFuture[(Long, Long)] = {
    try {
      getRunTimeInfoF(partition).thenApply { r: PartitionRuntimeInformation =>
        val earliest =
          if (r.getBeginSequenceNumber == -1L) 0L else r.getBeginSequenceNumber
        val latest = r.getLastEnqueuedSequenceNumber + 1
        (earliest, latest)
      }
    } catch {
      case e: Exception => throw e
    }
  }

  def earliestSeqNoF(partition: PartitionId): CompletableFuture[SequenceNumber] = {
    try {
      getRunTimeInfoF(partition).thenApply { r: PartitionRuntimeInformation =>
        val seqNo = r.getBeginSequenceNumber
        if (seqNo == -1L) 0L else seqNo
      }
    } catch {
      case e: Exception => throw e
    }
  }

  def latestSeqNoF(partition: PartitionId): CompletableFuture[SequenceNumber] = {
    try {
      getRunTimeInfoF(partition).thenApply { r: PartitionRuntimeInformation =>
        r.getLastEnqueuedSequenceNumber + 1
      }
    } catch {
      case e: Exception => throw e
    }
  }
   */

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
   * @return mapping of partitions to starting positions as sequence numbers
   */
  override def translate(ehConf: EventHubsConf,
                         partitionCount: Int,
                         useStart: Boolean = true): Map[PartitionId, SequenceNumber] = {

    val completed = mutable.Map[PartitionId, SequenceNumber]()
    val needsTranslation = ArrayBuffer[(NameAndPartition, EventPosition)]()

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

    (0 until partitionCount).par.foreach { id =>
      val nAndP = NameAndPartition(ehConf.name, id)
      val position = positions.getOrElse(nAndP, defaultPos)
      if (position.seqNo >= 0L) {
        // We don't need to translate a sequence number.
        // Put it straight into the results.
        synchronized(completed.put(id, position.seqNo))
      } else {
        val tuple = (nAndP, position)
        synchronized(needsTranslation += tuple)
      }
    }
    logInfo(s"translate: needsTranslation = $needsTranslation")

    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val threads = for ((nAndP, pos) <- needsTranslation)
      yield
        Future {
          pos.offset match {
            case StartOfStream => (nAndP.partitionId, earliestSeqNo(nAndP.partitionId))
            case EndOfStream   => (nAndP.partitionId, latestSeqNo(nAndP.partitionId))
            case _ =>
              retry(RetryCount) {
                val receiver =
                  client.createEpochReceiverSync(consumerGroup,
                                                 nAndP.partitionId.toString,
                                                 pos.convert,
                                                 DefaultEpoch)
                var event: java.lang.Iterable[EventData] = null
                while (event == null) {
                  event = receiver.receiveSync(1)
                }
                receiver.closeSync()
                (nAndP.partitionId, event.iterator.next.getSystemProperties.getSequenceNumber)
              }
          }
        }
    val sequenced = Future.sequence(threads).map(x => x.toMap ++ completed).map(identity)
    Await.result(sequenced, InternalOperationTimeout)
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
