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

import scala.collection.mutable.ListBuffer

private[spark] trait CachedReceiver {
  def receive(ehConf: EventHubsConf,
              nAndP: NameAndPartition,
              requestSeqNo: SequenceNumber,
              batchSize: Int): EventData
}

private class CachedEventHubsReceiver(ehConf: EventHubsConf, nAndP: NameAndPartition)
    extends Logging {

  import org.apache.spark.eventhubs._

  // Currently, we save the most recent batch size and adjust the
  // prefetch count accordingly. Leaving fixed list in place in case
  // we want to calculate a moving average in the future. To do so,
  // simply update the max FixedList size.
  val prefetchCounts = new FixedList[Int](1)

  private var _client: EventHubClient = _
  private def client: EventHubClient = {
    if (_client == null) {
      _client = ClientConnectionPool.borrowClient(ehConf)
    }
    _client
  }

  def createReceiver(requestSeqNo: SequenceNumber): Unit = {
    logInfo(s"creating receiver for Event Hub ${nAndP.ehName} on partition ${nAndP.partitionId}")
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val receiverOptions = new ReceiverOptions
    receiverOptions.setReceiverRuntimeMetricEnabled(false)
    receiverOptions.setIdentifier(s"${SparkEnv.get.executorId}-${TaskContext.get.taskAttemptId}")
    _receiver = client.createReceiverSync(consumerGroup,
                                          nAndP.partitionId.toString,
                                          EventPosition.fromSequenceNumber(requestSeqNo).convert,
                                          receiverOptions)
    _receiver.setPrefetchCount(DefaultPrefetchCount)
  }

  private[this] var _receiver: PartitionReceiver = _
  private def receiver: PartitionReceiver = {
    if (_receiver == null) {
      throw new IllegalStateException(s"No receiver for $nAndP")
    }
    _receiver
  }

  def errWrongSeqNo(requestSeqNo: SequenceNumber, receivedSeqNo: SequenceNumber): String =
    s"requestSeqNo $requestSeqNo does not match the received sequence number $receivedSeqNo"

  def receive(requestSeqNo: SequenceNumber, batchSize: Int): EventData = {
    @volatile var event: EventData = null
    @volatile var i: java.lang.Iterable[EventData] = null
    while (i == null) {
      i = receiver.receiveSync(1)
    }
    event = i.iterator.next

    if (requestSeqNo != event.getSystemProperties.getSequenceNumber) {
      logWarning(
        s"$requestSeqNo did not match ${event.getSystemProperties.getSequenceNumber}." +
          s"Recreating receiver for $nAndP")

      createReceiver(requestSeqNo)

      while (i == null) {
        i = receiver.receiveSync(1)
      }
      event = i.iterator.next
      assert(requestSeqNo == event.getSystemProperties.getSequenceNumber,
             errWrongSeqNo(requestSeqNo, event.getSystemProperties.getSequenceNumber))
    }

    logAndUpdatePrefetch(batchSize)
    event
  }

  private def logAndUpdatePrefetch(batchSize: Int): Unit = {
    prefetchCounts.append(batchSize)
    val avg = prefetchCounts.list.sum / prefetchCounts.list.size
    val updated = if (avg < PrefetchCountMinimum) { PrefetchCountMinimum * 2 } else { avg * 2 }
    receiver.setPrefetchCount(updated)
  }

  private[client] class FixedList[A](max: Int) {
    val list: ListBuffer[A] = ListBuffer()
    def append(elem: A) {
      if (list takeRight 1 contains elem) {
        // no-op, don't add duplicate batch size
      } else {
        if (list.size == max) { list.trimStart(1) }
        list.append(elem)
      }
    }
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

  private def get(nAndP: NameAndPartition): CachedEventHubsReceiver = receivers.synchronized {
    logInfo(s"lookup on $nAndP")
    receivers.getOrElse(nAndP, {
      val message = notInitializedMessage(nAndP)
      throw new IllegalStateException(message)
    })
  }

  override def receive(ehConf: EventHubsConf,
                       nAndP: NameAndPartition,
                       requestSeqNo: SequenceNumber,
                       batchSize: Int): EventData = {
    receivers.synchronized {
      if (!isInitialized(nAndP)) {
        receivers.update(nAndP, new CachedEventHubsReceiver(ehConf, nAndP))
        get(nAndP).createReceiver(requestSeqNo)
      }
    }

    val receiver = get(nAndP)
    receiver.receive(requestSeqNo, batchSize)
  }
}
