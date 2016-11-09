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
package org.apache.spark.streaming.eventhubs

import org.apache.spark.storage.StorageLevel

import scala.collection.Map
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver
import com.microsoft.azure.eventhubs._

import scala.util.control.ControlThrowable
import org.apache.spark.util.ThreadUtils

// Spark 1.6.x

private[eventhubs]
class EventHubsReceiver(
                         eventhubsParams: Map[String, String],
                         partitionId: String,
                         storageLevel: StorageLevel,
                         offsetStore: OffsetStore,
                         receiverClient: EventHubsClientWrapper,
                         maximumEventRate: Int
                       ) extends Receiver[Array[Byte]](storageLevel) with Logging {

  /** If offset store is empty we construct one using provided parameters */
  var myOffsetStore: OffsetStore = offsetStore

  if(myOffsetStore == null) {

    myOffsetStore = new DfsBasedOffsetStore(
      eventhubsParams("eventhubs.checkpoint.dir"),
      eventhubsParams("eventhubs.namespace"),
      eventhubsParams("eventhubs.name"),
      partitionId)
  }

  /** A state communicates between main thread and the MessageHandler thread.
    * Note we cannot use Receiver.isStopped() because there could be race condition when the
    * MessageHandler thread is started the state of the receiver has not been updated yet. */
  private var stopMessageHandler = false

  /** The latest sequence number this receiver has seen in messages from EventHubs.
    * It is used to throw away messages with backwards sequence number, to avoid duplicates
    * when receiver is restarted due to transient errors.
    * Note that Sequence number is monotonically increasing */
  //private var latestSequence: Long = Long.MinValue

  /** The offset to be saved after current checkpoint interval */
  protected var offsetToSave: String = _

  /** The last saved offset */
  protected var savedOffset: String = _

  def onStop() {
    logInfo("Stopping EventHubsReceiver for partition " + partitionId)
    stopMessageHandler = true

    // Don't need to do anything else here. Message handling thread will check stopMessageHandler
    // and close EventHubs client receiver.
  }

  def onStart() {
    logInfo("Starting EventHubsReceiver for partition " + partitionId)

    stopMessageHandler = false
    val executorPool = ThreadUtils.newDaemonFixedThreadPool(1, "EventHubsMessageHandler")

    try {

      executorPool.submit(new EventHubsMessageHandler)

    } finally {

      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  def processReceivedMessage(eventData: EventData): Unit = {

    // Just store the event data to Spark and update offsetToSave
    store(eventData.getBody)

    offsetToSave = eventData.getSystemProperties.getOffset
  }

  // Handles EventHubs messages
  private[eventhubs]
  class EventHubsMessageHandler()
    extends Runnable {

    // The checkpoint interval defaults to 10 seconds if not provided

    val checkpointInterval = if (eventhubsParams.contains("eventhubs.checkpoint.interval"))
      eventhubsParams("eventhubs.checkpoint.interval").toInt * 1000
    else 10000

    var nextCheckpointTime = System.currentTimeMillis() + checkpointInterval

    def run() {

      logInfo("Begin EventHubsMessageHandler for partition " + partitionId)

      try {

        myOffsetStore.open()

        // Create an EventHubs client receiver

        receiverClient.createReceiver(eventhubsParams, partitionId, myOffsetStore, maximumEventRate)

        var lastMaximumSequence: Long  = 0

        while (!stopMessageHandler) {

          val receivedEvents: Iterable[EventData] = receiverClient.receive()

          if (receivedEvents != null && receivedEvents.nonEmpty) {

            val eventCount = receivedEvents.count(x => x.getBodyLength > 0)
            val sequenceNumbers: Iterable[Long] = receivedEvents.map(x => x.getSystemProperties.getSequenceNumber)

            if (sequenceNumbers != null && sequenceNumbers.nonEmpty) {

              val maximumSequenceNumber: Long = sequenceNumbers.reduceLeft { (x, y) => if (x > y) x else y }
              val minimumSequenceNumber: Long = sequenceNumbers.reduceLeft { (x, y) => if (x < y) x else y }
              val missingSequenceCount: Long = maximumSequenceNumber - minimumSequenceNumber - eventCount + 1
              val sequenceNumberDiscontinuity: Long = minimumSequenceNumber - (lastMaximumSequence + 1)

              lastMaximumSequence = maximumSequenceNumber

              logDebug(s"Partition Id: $partitionId, Event Count: $eventCount," +
                s" Maximum Sequence Number: $maximumSequenceNumber, Minimum Sequence Number: $minimumSequenceNumber," +
                s" Missing Sequence Count: $missingSequenceCount," +
                s" Sequence Number Discontinuity = $sequenceNumberDiscontinuity")
            }
            else {

              logDebug(s"Partition Id: $partitionId, Event Count: $eventCount")
            }

            receivedEvents.foreach(x => processReceivedMessage(x))
          }

          val currentTime = System.currentTimeMillis()

          if(currentTime >= nextCheckpointTime) {

            if(offsetToSave != savedOffset) {
              logInfo(s"Partition Id: $partitionId, Current Time: $currentTime," +
                s" Next Checkpoint Time: $nextCheckpointTime, Saved Offset: $offsetToSave")

              myOffsetStore.write(offsetToSave)
              savedOffset = offsetToSave
              nextCheckpointTime = currentTime + checkpointInterval
            }
          }
        }
      } catch {

        //case c: ControlThrowable => throw c // propagate these bad throwable
        case e: Throwable => restart(s"Error handling message, restarting receiver for partition $partitionId", e)

      } finally {

        myOffsetStore.close()
        receiverClient.close()
        logInfo("End EventHubsMessageHandler for partition " + partitionId)
      }
    }
  }
}
