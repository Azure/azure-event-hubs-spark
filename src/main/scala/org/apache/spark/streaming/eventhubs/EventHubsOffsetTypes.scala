package org.apache.spark.streaming.eventhubs

object EventhubsOffsetType extends Enumeration {

  type EventhubsOffsetType = Value

  val None, PreviousCheckpoint, InputByteOffset, InputTimeOffset = Value
}