package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.{NameAndPartition, SequenceNumber}

trait EventHubsReceiverListener extends Serializable {

  def onBatchReceiveSuccess(nAndP: NameAndPartition, elapsedTime: Long, batchSize: Int, receivedBytes: Long): Unit

  def onBatchReceiveSkip(nAndP: NameAndPartition, requestSeqNo: SequenceNumber, batchSize: Int): Unit

  def onReceiveFirstEvent(nAndP: NameAndPartition, firstEvent: EventData): Unit

}
