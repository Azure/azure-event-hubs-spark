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
    var client = pool.poll()
    if (client == null) {
      logInfo(
        s"No clients left to borrow. EventHub name: ${ehConf.name}. Creating client ${count.incrementAndGet()}")
      val connStr = ConnectionStringBuilder(ehConf.connectionString)
      connStr.setOperationTimeout(ehConf.operationTimeout.getOrElse(DefaultOperationTimeout))
      while (client == null) {
        client = EventHubClient.createSync(connStr.toString, ClientThreadPool.pool)
      }
    } else {
      logInfo(s"Borrowing client. EventHub name: ${ehConf.name}")
    }
    logInfo(s"Available clients: {${pool.size}}. Total clients: ${count.get}")

    client
  }

  private def returnClient(client: EventHubClient): Unit = {
    pool.offer(client)
    logInfo(
      s"Client returned. EventHub name: ${ehConf.name}. Total clients: ${count.get}. Available clients: ${pool.size}")
  }
}

/**
  * Represents an object being pooled in {@link ClientConnectionPool}
  * @param ehConf The Event Hubs configurations corresponding to the EventHubClient.
  * @param ehClient The EventHubClient to be pooled.
  */
class ClientConnectionPoolObject(private[this] val ehConf: EventHubsConf, private[this] val ehClient: EventHubClient) {

  def getEventHubsConf: EventHubsConf = {
    return ehConf;
  }

  def getEventHubClient: EventHubClient = {
    return ehClient;
  }
}

object ClientConnectionPool extends Logging {

  private def notInitializedMessage(name: String): String = {
    s"Connection pool is not initialized for EventHub: ${name}"
  }

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val pools = new MutableMap[String, ClientConnectionPool]()

  private def isInitialized(hashKey: String): Boolean = pools.synchronized {
    pools.get(hashKey).isDefined
  }

  private def ensureInitialized(ehConf: EventHubsConf): Unit = {
    val hashKey = ehConf.connectionString
    if (!isInitialized(hashKey)) {
      val message = notInitializedMessage(ehConf.name)
      throw new IllegalStateException(message)
    }
  }

  private def get(ehConf: EventHubsConf): ClientConnectionPool = pools.synchronized {
    val hashKey = ehConf.connectionString
    pools.getOrElse(hashKey, {
      val message = notInitializedMessage(ehConf.name)
      throw new IllegalStateException(message)
    })
  }

  def borrowClient(ehConf: EventHubsConf): ClientConnectionPoolObject = {
    val hashKey = ehConf.connectionString
    if (!isInitialized(hashKey)) {
      pools.synchronized {
        pools.update(hashKey, new ClientConnectionPool(ehConf))
      }
    }

    val pool = get(ehConf)
    val ehClient = pool.borrowClient
    new ClientConnectionPoolObject(ehConf, ehClient)
  }

  def returnClient(client: ClientConnectionPoolObject): Unit = {
    val ehConf = client.getEventHubsConf
    val ehClient = client.getEventHubClient

    ensureInitialized(ehConf)

    val pool = get(ehConf)
    pool.returnClient(ehClient)
  }
}
