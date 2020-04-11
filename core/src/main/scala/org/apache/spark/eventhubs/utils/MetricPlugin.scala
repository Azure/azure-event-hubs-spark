package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.{NameAndPartition, SequenceNumber}

trait MetricPlugin extends Serializable {

  def onReceiveMetric(partitionInfo: NameAndPartition, batchCount: Int, batchSizeInBytes: Long,  elapsedTimeInMillis: Long): Unit

  def onSendMetric(eventHubName: String, batchCount: Int, batchSizeInBytes: Long,  elapsedTimeInMillis: Long, isSuccess: Boolean): Unit

}
