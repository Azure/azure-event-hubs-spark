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

import com.microsoft.azure.eventhubs.{
  EventData,
  EventHubClient,
  PartitionReceiver,
  ReceiverOptions
}
import org.apache.spark.{ SparkEnv, TaskContext }
import org.apache.spark.eventhubs.{ EventHubsConf, NameAndPartition, SequenceNumber }
import org.apache.spark.internal.Logging

private[spark] trait CachedReceiver {
  private[eventhubs] def receive(ehConf: EventHubsConf,
                                 nAndP: NameAndPartition,
                                 requestSeqNo: SequenceNumber,
                                 batchSize: Int): EventData
}

private[client] class CachedEventHubsReceiver private (ehConf: EventHubsConf,
                                                       nAndP: NameAndPartition)
    extends Logging {

  import org.apache.spark.eventhubs._

  private var _client: EventHubClient = _
  private def client: EventHubClient = {
    if (_client == null) {
      _client = ClientConnectionPool.borrowClient(ehConf)
    }
    _client
  }

  private def createReceiver(requestSeqNo: SequenceNumber): Unit = {
    logInfo(s"creating receiver for Event Hub ${nAndP.ehName} on partition ${nAndP.partitionId}")
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val receiverOptions = new ReceiverOptions
    receiverOptions.setReceiverRuntimeMetricEnabled(false)
    receiverOptions.setIdentifier(
      s"spark-${SparkEnv.get.executorId}-${TaskContext.get.taskAttemptId}")
    _receiver = client.createReceiverSync(consumerGroup,
                                          nAndP.partitionId.toString,
                                          EventPosition.fromSequenceNumber(requestSeqNo).convert,
                                          receiverOptions)
    _receiver.setPrefetchCount(DefaultPrefetchCount)
  }

  private var _receiver: PartitionReceiver = _
  private def receiver: PartitionReceiver = {
    if (_receiver == null) {
      throw new IllegalStateException(s"No receiver for $nAndP")
    }
    _receiver
  }

  private def errWrongSeqNo(requestSeqNo: SequenceNumber, receivedSeqNo: SequenceNumber): String =
    s"requestSeqNo $requestSeqNo does not match the received sequence number $receivedSeqNo"

  private def receive(requestSeqNo: SequenceNumber, batchSize: Int): EventData = {
    var event: EventData = null
    var i: java.lang.Iterable[EventData] = null
    while (i == null) { i = receiver.receiveSync(1) }
    event = i.iterator.next

    if (requestSeqNo != event.getSystemProperties.getSequenceNumber) {
      logWarning(
        s"$requestSeqNo did not match ${event.getSystemProperties.getSequenceNumber}." +
          s"Recreating receiver for $nAndP")
      createReceiver(requestSeqNo)
      while (i == null) { i = receiver.receiveSync(1) }
      event = i.iterator.next
      assert(requestSeqNo == event.getSystemProperties.getSequenceNumber,
             errWrongSeqNo(requestSeqNo, event.getSystemProperties.getSequenceNumber))
    }
    receiver.setPrefetchCount(batchSize)
    event
  }
}

private[spark] object CachedEventHubsReceiver extends CachedReceiver with Logging {

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val receivers = new MutableMap[String, CachedEventHubsReceiver]()

  private def key(ehConf: EventHubsConf, nAndP: NameAndPartition): String = {
    (ehConf.connectionString + ehConf.consumerGroup + nAndP.partitionId).toLowerCase
  }

  private[eventhubs] override def receive(ehConf: EventHubsConf,
                                          nAndP: NameAndPartition,
                                          requestSeqNo: SequenceNumber,
                                          batchSize: Int): EventData = {
    var receiver: CachedEventHubsReceiver = null
    receivers.synchronized {
      receiver = receivers.getOrElseUpdate(key(ehConf, nAndP), {
        CachedEventHubsReceiver(ehConf, nAndP, requestSeqNo)
      })
    }
    receiver.receive(requestSeqNo, batchSize)
  }

  def apply(ehConf: EventHubsConf,
            nAndP: NameAndPartition,
            startSeqNo: SequenceNumber): CachedEventHubsReceiver = {
    val cr = new CachedEventHubsReceiver(ehConf, nAndP)
    cr.createReceiver(startSeqNo)
    cr
  }
}
