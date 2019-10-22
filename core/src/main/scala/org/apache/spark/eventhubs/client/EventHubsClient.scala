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

import com.microsoft.azure.eventhubs._
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl
import org.apache.spark.SparkEnv
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.utils.RetryUtils._
import org.apache.spark.internal.Logging
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ ArrayBuffer, ListBuffer }
import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }
import scala.util.{ Failure, Success }

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

  private var pendingWorks = new ListBuffer[Future[Any]]

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
                    partitionKey: Option[String] = None,
                    properties: Option[Map[String, String]] = None): Unit = {
    if (properties.isDefined) {
      val p = event.getProperties
      p.putAll(properties.get.asJava)
    }

    val sendTask = if (partition.isDefined) {
      if (partitionSender.getPartitionId.toInt != partition.get) {
        logInfo("Recreating partition sender.")
        createPartitionSender(partition.get)
      }
      partitionSender.send(event)
    } else if (partitionKey.isDefined) {
      client.send(event, partitionKey.get)
    } else {
      client.send(event)
    }

    pendingWorks += FutureConverters.toScala(sendTask)
  }

  /**
   * Retrieves partition runtime information for a specific partition.
   *
   * @param partitionId the partition to be queried.
   * @return [[PartitionRuntimeInformation]] for the partition
   */
  private def getRunTimeInfoF(partitionId: PartitionId): Future[PartitionRuntimeInformation] = {
    retryJava(client.getPartitionRuntimeInformation(partitionId.toString),
              s"getRunTimeInfoF for partition: $partitionId")
  }

  /**
   * Same as boundedSeqNos, but for all partitions in the Event Hub.
   *
   * @return the earliest and latest sequence numbers for all partitions in the Event Hub
   */
  override def allBoundedSeqNos: Map[PartitionId, (SequenceNumber, SequenceNumber)] = {
    val futures = for (i <- 0 until partitionCount)
      yield
        getRunTimeInfoF(i) map { r =>
          val earliest =
            if (r.getBeginSequenceNumber == -1L) 0L
            else {
              if (r.getIsEmpty) r.getLastEnqueuedSequenceNumber + 1 else r.getBeginSequenceNumber
            }
          val latest = r.getLastEnqueuedSequenceNumber + 1
          i -> (earliest, latest)
        }
    Await
      .result(Future.sequence(futures), ehConf.internalOperationTimeout)
      .toMap
  }

  /**
   * Provides a [[Future]] containing the earliest (lowest) sequence number
   * that exists in the EventHubs instance for the given partition.
   *
   * @param partition the partition that will be queried
   * @return A [[Future]] containing the earliest sequence number for the specified partition
   */
  private def earliestSeqNoF(partition: PartitionId): Future[SequenceNumber] = {
    getRunTimeInfoF(partition).map { r =>
      val seqNo =
        if (r.getIsEmpty) r.getLastEnqueuedSequenceNumber + 1 else r.getBeginSequenceNumber
      if (seqNo == -1L) 0L else seqNo
    }
  }

  /**
   * Provides a [[Future]] containing the latest (highest) sequence number that
   * exists in the EventHubs instance for the given partition.
   *
   * @param partition the partition that will be queried
   * @return a [[Future]] containing the latest sequence number for the specified partition
   */
  private def latestSeqNoF(partition: PartitionId): Future[SequenceNumber] =
    getRunTimeInfoF(partition).map(_.getLastEnqueuedSequenceNumber + 1)

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

    Future.sequence(pendingWorks).onComplete {
      case Success(_) => cleanup()
      case Failure(e) =>
        logError(s"failed to complete pending tasks. $ehConf: ", e)
        cleanup()

        throw e
    }
  }

  private def cleanup(): Unit = {
    pendingWorks.clear()

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
   * @param ehConf         the [[EventHubsConf]] containing starting (or ending positions)
   * @param partitionCount the number of partitions in the Event Hub instance
   * @param useStart       translates starting positions when true and ending positions
   *                       when false
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
    val futures = for ((nAndP, pos) <- needsTranslation)
      yield
        pos.offset match {
          case StartOfStream => (nAndP.partitionId, earliestSeqNoF(nAndP.partitionId))
          case EndOfStream   => (nAndP.partitionId, latestSeqNoF(nAndP.partitionId))
          case _ =>
            val runtimeInfo =
              Await.result(getRunTimeInfoF(nAndP.partitionId), ehConf.internalOperationTimeout)
            var receiver: Future[PartitionReceiver] = null
            val seqNo =
              if (runtimeInfo.getIsEmpty || (pos.enqueuedTime != null &&
                  runtimeInfo.getLastEnqueuedTimeUtc.isBefore(pos.enqueuedTime.toInstant))) {
                Future.successful(runtimeInfo.getLastEnqueuedSequenceNumber + 1)
              } else {
                logInfo(
                  s"translate: creating receiver for Event Hub ${nAndP.ehName} on partition ${nAndP.partitionId}. filter: ${pos.convert}")
                val receiverOptions = new ReceiverOptions
                receiverOptions.setPrefetchCount(1)
                receiverOptions.setIdentifier(s"spark-${SparkEnv.get.executorId}")

                receiver = retryJava(
                  EventHubsUtils.createReceiverInner(client,
                                                     ehConf.useExclusiveReceiver,
                                                     consumerGroup,
                                                     nAndP.partitionId.toString,
                                                     pos.convert,
                                                     receiverOptions),
                  "translate: receiver creation."
                )
                receiver
                  .flatMap { r =>
                    r.setReceiveTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
                    retryNotNull(r.receive(1), "translate: receive call")
                  }
                  .map { e =>
                    e.iterator.next.getSystemProperties.getSequenceNumber
                  }
              }
            if (receiver != null) {
              receiver
                .flatMap { r =>
                  Future.successful(r.close())
                }
            }
            (nAndP.partitionId, seqNo)
        }

    val future = Future
      .traverse(futures) {
        case (p, f) =>
          f.map { seqNo =>
            (p, seqNo)
          }
      }
      .map(x => x.toMap ++ completed)
      .map(identity)
    Await.result(future, ehConf.internalOperationTimeout)
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
