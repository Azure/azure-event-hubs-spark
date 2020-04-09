package org.apache.spark.eventhubs.utils

import org.apache.spark.eventhubs.{NameAndPartition, Rate, SequenceNumber}

class MetricPluginMock extends MetricPlugin {

    val id = 1

    override def onReceiveMetric(partitionInfo: NameAndPartition,
                                 batchCount: Rate,
                                 batchSizeInBytes: SequenceNumber,
                                 elapsedTimeInMillis: SequenceNumber): Unit = ???

    override def onSendMetric(batchCount: Rate,
                              batchSizeInBytes: SequenceNumber,
                              elapsedTimeInMillis: SequenceNumber,
                              isSuccess: Boolean): Unit = ???
}