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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import com.microsoft.azure.eventhubs.EventHubClient
import org.apache.spark.eventhubs._
import org.apache.spark.internal.Logging

/**
 * A connection pool for EventHubClients. If a connection isn't available in the pool, then
 * a new one is created. If a connection idles in the pool for 5 minutes, it will be closed.
 *
 * @param ehConf The Event Hubs configurations corresponding to this specific connection pool.
 */
private class ClientConnectionPool(val ehConf: EventHubsConf) extends Logging {

  private[this] val pool = new ConcurrentLinkedQueue[EventHubClient]()
  private[this] val count = new AtomicInteger(0)

  private def borrowClient: EventHubClient = {
    val client = pool.poll()
    if (client == null) {
      logInfo(
        s"No clients left to borrow. EventHub name: ${ehConf.name}. Creating client ${count.incrementAndGet()}")
      val connStr = ConnectionStringBuilder(ehConf.connectionString)
      connStr.setOperationTimeout(ehConf.operationTimeout.getOrElse(DefaultOperationTimeout))
      EventHubClient.createSync(connStr.toString, ClientThreadPool.pool)
    } else {
      logInfo(s"Borrowing client. EventHub name: ${ehConf.name}")
      client
    }
  }

  private def returnClient(client: EventHubClient): Unit = {
    pool.offer(client)
    logInfo(
      s"Client returned. EventHub name: ${ehConf.name}. Total clients: ${count.get}. Available clients: ${pool.size}")
  }
}

object ClientConnectionPool extends Logging {

  private def notInitializedMessage(name: String): String = {
    s"Connection pool is not initialized for EventHubs: $name"
  }

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val pools = new MutableMap[String, ClientConnectionPool]()

  def isInitialized(name: String): Boolean = pools.synchronized {
    pools.get(name).isDefined
  }

  private def ensureInitialized(name: String): Unit = {
    if (!isInitialized(name)) {
      val message = notInitializedMessage(name)
      throw new IllegalStateException(message)
    }
  }

  private def get(name: String): ClientConnectionPool = pools.synchronized {
    pools.getOrElse(name, {
      val message = notInitializedMessage(name)
      throw new IllegalStateException(message)
    })
  }

  def borrowClient(ehConf: EventHubsConf): EventHubClient = {
    val name = ehConf.name

    if (!isInitialized(name)) {
      pools.synchronized {
        pools.update(name, new ClientConnectionPool(ehConf))
      }
    }

    val pool = get(name)
    pool.borrowClient
  }

  def returnClient(client: EventHubClient): Unit = {
    val name = client.getEventHubName
    ensureInitialized(name)
    val pool = get(name)
    pool.returnClient(client)
  }
}
