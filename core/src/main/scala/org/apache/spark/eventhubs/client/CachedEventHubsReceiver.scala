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
  def receive(ehConf: EventHubsConf,
              nAndP: NameAndPartition,
              requestSeqNo: SequenceNumber): EventData
}

// TODO: Figure out correct prefetch count
// TODO: Benefits of a smart buffer?
private class CachedEventHubsReceiver(ehConf: EventHubsConf,
                                      nAndP: NameAndPartition,
                                      startingSeqNo: SequenceNumber)
    extends Logging {

  import org.apache.spark.eventhubs._

  val prefetchCount = 2000
  var requestSeqNo: SequenceNumber = startingSeqNo

  private var _client: EventHubClient = _
  private def client: EventHubClient = {
    if (_client == null) {
      _client = ClientConnectionPool.borrowClient(ehConf)
    }
    _client
  }

  private[this] var _receiver: PartitionReceiver = _
  private def receiver: PartitionReceiver = {
    if (_receiver == null) {
      val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
      val receiverOptions = new ReceiverOptions
      receiverOptions.setReceiverRuntimeMetricEnabled(false)
      receiverOptions.setIdentifier(s"${SparkEnv.get.executorId}-${TaskContext.get.taskAttemptId}")
      _receiver = client.createReceiverSync(consumerGroup,
                                            nAndP.partitionId.toString,
                                            EventPosition.fromSequenceNumber(startingSeqNo).convert,
                                            receiverOptions)
      _receiver.setPrefetchCount(prefetchCount)
    }
    _receiver
  }

  def errWrongSeqNo(receivedSeqNo: SequenceNumber): String =
    s"requestSeqNo $requestSeqNo does not match the received sequence number $receivedSeqNo"

  def receive: EventData = {
    @volatile var event: EventData = null
    @volatile var i: java.lang.Iterable[EventData] = null
    while (i == null) {
      i = receiver.receiveSync(1)
    }
    event = i.iterator.next

    assert(requestSeqNo == event.getSystemProperties.getSequenceNumber,
           errWrongSeqNo(event.getSystemProperties.getSequenceNumber))
    requestSeqNo += 1
    event
  }
}

private[spark] object CachedEventHubsReceiver extends CachedReceiver with Logging {

  private def notInitializedMessage(nAndP: NameAndPartition): String = {
    s"No receiver found for EventHubs ${nAndP.ehName} on partition ${nAndP.partitionId}"
  }

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val receivers = new MutableMap[NameAndPartition, CachedEventHubsReceiver]()

  def isInitialized(nAndP: NameAndPartition): Boolean = receivers.synchronized {
    receivers.get(nAndP).isDefined
  }

  private def ensureInitialized(nAndP: NameAndPartition): Unit = {
    if (!isInitialized(nAndP)) {
      val message = notInitializedMessage(nAndP)
      throw new IllegalStateException(message)
    }
  }

  private def get(nAndP: NameAndPartition): CachedEventHubsReceiver = receivers.synchronized {
    receivers.getOrElse(nAndP, {
      val message = notInitializedMessage(nAndP)
      throw new IllegalStateException(message)
    })
  }

  override def receive(ehConf: EventHubsConf,
                       nAndP: NameAndPartition,
                       requestSeqNo: SequenceNumber): EventData = {
    receivers.synchronized {
      if (!isInitialized(nAndP)) {
        receivers.update(nAndP, new CachedEventHubsReceiver(ehConf, nAndP, requestSeqNo))
      }
    }

    val receiver = get(nAndP)
    receiver.receive
  }
}
