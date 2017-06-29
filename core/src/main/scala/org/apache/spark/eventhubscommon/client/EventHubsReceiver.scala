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

package org.apache.spark.eventhubscommon.client

import com.microsoft.azure.eventhubs.EventData

import org.apache.spark.eventhubscommon.EventHubNameAndPartition

private[spark] trait EventHubsReceiver[T] {

  def receive(receiver: T, expectedEventNum: Int): Iterable[EventData]

  def closeClient(receiver: T): Unit

  def closeReceiver(receiver: T, ehNameAndPartition: EventHubNameAndPartition): Unit

}

private[spark] object EventHubsReceiver {

  implicit object eventHubsReceiverWrapper extends EventHubsReceiver[EventHubsReceiverWrapper] {
    override def receive(receiver: EventHubsReceiverWrapper, expectedEventNum: Int):
      Iterable[EventData] = {
      receiver.receive(expectedEventNum)
    }

    override def closeClient(receiver: EventHubsReceiverWrapper): Unit = {
      receiver.close()
    }

    override def closeReceiver(
                                receiver: EventHubsReceiverWrapper,
                                ehNameAndPartition: EventHubNameAndPartition): Unit = {
      receiver.closeReceiver()
    }
  }
}
