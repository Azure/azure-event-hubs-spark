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

package org.apache.spark.eventhubs.rdd

import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.client.Client
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer

private[spark] class EventHubsRDD(sc: SparkContext,
                                  val ehConf: EventHubsConf,
                                  val offsetRanges: Array[OffsetRange],
                                  receiverFactory: (EventHubsConf => Client))
    extends RDD[EventData](sc, Nil)
    with Logging
    with HasOffsetRanges {

  import org.apache.spark.eventhubs._

  override def getPartitions: Array[Partition] = {
    log.info(s"Generating ${ehConf.parallelTasksPerPartition} tasks per ${offsetRanges.size} offsets")
    for {
      (o, i) <- offsetRanges.flatMap(or => split(or, ehConf.parallelTasksPerPartition)).zipWithIndex
    } yield
      new EventHubsRDDPartition(i, o.nameAndPartition, o.fromSeqNo, o.untilSeqNo, o.preferredLoc)
  }

  private def split(offset:OffsetRange, chunks:Int):Seq[OffsetRange]  = {
    if(chunks == 1){
      Seq(offset)
    }
    val groups = splitRange(offset.fromSeqNo until offset.untilSeqNo, 5, 2)
    val groupTuples = groups.map(f => (f.start, f.end))
    groupTuples.map(r => {
      OffsetRange(offset.nameAndPartition, r._1, r._2, offset.preferredLoc)
    }).toSeq
  }

  private def splitRange(r: NumericRange[Long], chunks: Int, minimum:Int): Seq[NumericRange[Long]] = {
    if (r.step != 1)
      throw new IllegalArgumentException("Range must have step size equal to 1")

    val nchunks = scala.math.max(chunks, 1)
    val chunkSize = scala.math.max(r.length / nchunks, 1)
    val starts = r.by(chunkSize).take(nchunks)
    val ends = starts.drop(1) :+ r.end
    val ranges = starts.zip(ends).map(x => x._1 until x._2)
    if(ranges.exists(_.size < minimum) && chunks > 0) {
      splitRange(r, chunks - 1, minimum)
    } else if (chunks > 0) {
      ranges
    } else {
      Seq(r)
    }
  }

  override def count: Long = offsetRanges.map(_.count).sum

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[EventData] = {
    val nonEmptyPartitions =
      this.partitions.map(_.asInstanceOf[EventHubsRDDPartition]).filter(_.count > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return Array()
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

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[EventHubsRDDPartition]
    part.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
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

    val client: Client = receiverFactory(ehConf)
    client.createReceiver(part.partitionId.toString, part.fromSeqNo)

    val prefetchCount =
      if (part.count.toInt < PrefetchCountMinimum) PrefetchCountMinimum else part.count.toInt
    client.setPrefetchCount(prefetchCount)

    var requestSeqNo: SequenceNumber = part.fromSeqNo

    override def hasNext(): Boolean = requestSeqNo < part.untilSeqNo

    def errWrongSeqNo(part: EventHubsRDDPartition, receivedSeqNo: SequenceNumber): String =
      s"requestSeqNo $requestSeqNo does not match the received sequence number $receivedSeqNo"

    override def next(): EventData = {
      assert(hasNext(), "Can't call next() once untilSeqNo has been reached.")

      @volatile var event: EventData = null
      @volatile var i: java.lang.Iterable[EventData] = null
      while (i == null) {
        i = client.receive(1)
      }
      event = i.iterator.next

      assert(requestSeqNo == event.getSystemProperties.getSequenceNumber,
             errWrongSeqNo(part, event.getSystemProperties.getSequenceNumber))
      requestSeqNo += 1
      event
    }

    context.addTaskCompletionListener { _ =>
      closeIfNeeded()
    }

    def closeIfNeeded(): Unit = {
      if (client != null) client.close()
    }
  }
}
