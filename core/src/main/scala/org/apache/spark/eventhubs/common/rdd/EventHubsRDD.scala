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

package org.apache.spark.eventhubs.common.rdd

import scala.collection.mutable.ArrayBuffer
import com.microsoft.azure.eventhubs.{ EventData, PartitionReceiver }
import org.apache.spark.eventhubs.common.{ EventHubsConf, SequenceNumber }
import org.apache.spark.eventhubs.common.client.Client
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }

private[spark] class EventHubsRDD(sc: SparkContext,
                                  ehConf: EventHubsConf,
                                  val offsetRanges: Array[OffsetRange],
                                  receiverFactory: (EventHubsConf => Client))
    extends RDD[EventData](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    for {
      (o, i) <- offsetRanges.zipWithIndex
    } yield new EventHubsRDDPartition(i, o.nameAndPartition, o.fromSeqNo, o.untilSeqNo)
  }

  override def count(): Long = offsetRanges.map(_.count).sum

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[EventData] = {
    val nonEmptyPartitions =
      this.partitions.map(_.asInstanceOf[EventHubsRDDPartition]).filter(_.count > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return new Array[EventData](0)
    }

    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.count)
        result + (part.index -> taken.toInt)

      } else {
        result
      }
    }

    val buf = new ArrayBuffer[EventData]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[EventData]) => it.take(parts(tc.partitionId)).toArray,
      parts.keys.toArray)
    res.foreach(buf ++= _)
    buf.toArray
  }

  private def errBeginAfterEnd(part: EventHubsRDDPartition): String =
    s"The beginning sequence number ${part.fromSeqNo} is larger than thet ending sequence number ${part.untilSeqNo}" +
      s"for EventHubs ${part.name} on partitionId ${part.partitionId}."

  override def compute(partition: Partition, context: TaskContext): Iterator[EventData] = {
    val part = partition.asInstanceOf[EventHubsRDDPartition]
    assert(part.fromSeqNo <= part.untilSeqNo, errBeginAfterEnd(part))

    if (part.fromSeqNo == part.untilSeqNo) {
      logInfo(
        s"Beginning sequence number ${part.fromSeqNo} is equal to the ending sequence number ${part.untilSeqNo}." +
          s"Returning empty partition for EH: ${part.name} on partition: ${part.partitionId}")
      Iterator.empty
    } else {
      new EventHubsRDDIterator(part, context)
    }
  }

  private class EventHubsRDDIterator(part: EventHubsRDDPartition, context: TaskContext)
      extends Iterator[EventData] {

    logInfo(
      s"Computing EventHubs ${part.name}, partitionId ${part.partitionId} " +
        s"sequence numbers ${part.fromSeqNo} => ${part.untilSeqNo}")

    val receiver: PartitionReceiver =
      receiverFactory(ehConf).receiver(part.partitionId.toString, part.fromSeqNo)
    receiver.setPrefetchCount(part.count.toInt)

    var requestSeqNo: SequenceNumber = part.fromSeqNo

    override def hasNext(): Boolean = requestSeqNo < part.untilSeqNo

    def errWrongSeqNo(part: EventHubsRDDPartition, receivedSeqNo: SequenceNumber): String =
      s"requestSeqNo $requestSeqNo does not match the received sequence number $receivedSeqNo"

    override def next(): EventData = {
      assert(hasNext(), "Can't call next() once untilSeqNo has been reached.")
      val event = receiver.receive(1).get.iterator().next()
      assert(requestSeqNo == event.getSystemProperties.getSequenceNumber,
             errWrongSeqNo(part, event.getSystemProperties.getSequenceNumber))
      requestSeqNo += 1
      event
    }

    context.addTaskCompletionListener { _ =>
      closeIfNeeded()
    }

    def closeIfNeeded(): Unit = {
      if (receiver != null) receiver.close()
    }
  }
}
