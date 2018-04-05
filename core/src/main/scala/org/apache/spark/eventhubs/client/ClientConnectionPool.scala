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

import java.util.concurrent.{ ConcurrentLinkedQueue, ExecutorService, Executors }
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

object ClientConnectionPool extends Logging {

  private def notInitializedMessage(name: String): String = {
    s"Connection pool is not initialized for EventHubs: $name"
  }

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val pools = new MutableMap[String, ClientConnectionPool]()

  def isInitialized(key: String): Boolean = pools.synchronized {
    pools.get(key).isDefined
  }

  private def ensureInitialized(key: String): Unit = {
    if (!isInitialized(key)) {
      val message = notInitializedMessage(key)
      throw new IllegalStateException(message)
    }
  }

  private def key(ehConf: EventHubsConf): String = {
    ehConf.connectionString.toLowerCase
  }

  private def get(key: String): ClientConnectionPool = pools.synchronized {
    pools.getOrElse(key, {
      val message = notInitializedMessage(key)
      throw new IllegalStateException(message)
    })
  }

  def borrowClient(ehConf: EventHubsConf): EventHubClient = {
    pools.synchronized {
      if (!isInitialized(key(ehConf))) {
        pools.update(key(ehConf), new ClientConnectionPool(ehConf))
      }
    }

    val pool = get(key(ehConf))
    pool.borrowClient
  }

  def returnClient(ehConf: EventHubsConf, client: EventHubClient): Unit = {
    ensureInitialized(key(ehConf))
    val pool = get(key(ehConf))
    pool.returnClient(client)
  }
}

/**
 * Thread pool for EventHub client. We create one thread pool per JVM.
 * Threads are created on demand if none are available.
 * In future releases, thread pool will be configurable by users.
 */
object ClientThreadPool {
  val pool: ExecutorService = Executors.newCachedThreadPool
}
