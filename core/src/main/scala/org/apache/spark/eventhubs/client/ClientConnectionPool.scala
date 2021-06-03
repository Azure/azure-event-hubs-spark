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

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ConcurrentLinkedQueue, Executors, ScheduledExecutorService }

import com.microsoft.azure.eventhubs.{ EventHubClient, EventHubClientOptions, RetryPolicy }
import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.utils.RetryUtils.retryJava
import org.apache.spark.internal.Logging

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * A connection pool for EventHubClients. A connection pool is created per connection string.
 * If a connection isn't available in the pool, then a new one is created.
 * If a connection is idle in the pool for 5 minutes, it will be closed.
 *
 * @param ehConf The Event Hubs configurations corresponding to this specific connection pool.
 */
private class ClientConnectionPool(val ehConf: EventHubsConf) extends Logging {

  private[this] val pool = new ConcurrentLinkedQueue[EventHubClient]()
  private[this] val count = new AtomicInteger(0)

  /**
   * First, the connection pool is checked for available clients. If there
   * is a client available, that is given to the borrower. If the connection
   * pool is empty, then a new client is created and given to the borrower.
   *
   * @return the borrowed [[EventHubClient]]
   */
  private def borrowClient: EventHubClient = {
    var client = pool.poll()
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    if (client == null) {
      logInfo(
        s"No clients left to borrow. NamespaceUri: ${ehConf.namespaceUri}, EventHub name: ${ehConf.name}, " +
          s"ConsumerGroup name: $consumerGroup. Creating client ${count.incrementAndGet()}")
      val connStr = ConnectionStringBuilder(ehConf.connectionString)
      connStr.setOperationTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
      EventHubsClient.userAgent =
        s"SparkConnector-$SparkConnectorVersion-[${ehConf.name}]-[$consumerGroup]"
      while (client == null) {
        if (ehConf.useAadAuth) {
          val ehClientOption: EventHubClientOptions = new EventHubClientOptions()
            .setMaximumSilentTime(ehConf.maxSilentTime.getOrElse(DefaultMaxSilentTime))
            .setOperationTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
            .setRetryPolicy(RetryPolicy.getDefault)
          client = Await.result(
            retryJava(
              EventHubClient
                .createWithAzureActiveDirectory(connStr.getEndpoint,
                                                ehConf.name,
                                                ehConf.aadAuthCallback().get,
                                                ehConf.aadAuthCallback().get.authority,
                                                ClientThreadPool.get(ehConf),
                                                ehClientOption),
              "createWithAzureActiveDirectory"
            ),
            ehConf.internalOperationTimeout
          )
        } else {
          client = Await.result(
            retryJava(
              EventHubClient.createFromConnectionString(
                connStr.toString,
                RetryPolicy.getDefault,
                ClientThreadPool.get(ehConf),
                null,
                ehConf.maxSilentTime.getOrElse(DefaultMaxSilentTime)),
              "createFromConnectionString"
            ),
            ehConf.internalOperationTimeout
          )
        }
      }
    } else {
      logInfo(
        s"Borrowing client. Namespace: ${ehConf.namespaceUri}, EventHub name: ${ehConf.name}, ConsumerGroup name: " +
          s"$consumerGroup")
    }
    logInfo(s"Available clients: {${pool.size}}. Total clients: ${count.get}")
    client
  }

  /**
   * Returns the borrowed client to the connection pool.
   *
   * @param client the [[EventHubClient]] being returned
   */
  private def returnClient(client: EventHubClient): Unit = {
    pool.offer(client)
    logInfo(
      s"Client returned. Namespace: ${ehConf.namespaceUri},EventHub name: ${ehConf.name}. Total clients: ${count.get}. " +
        s"Available clients: ${pool.size}")
  }
}

/**
 * The connection pool singleton that is created per JVM. This holds a map
 * of Event Hubs connection strings to their specific connection pool instances.
 */
object ClientConnectionPool extends Logging {

  private def notInitializedMessage(name: String): String = {
    s"Connection pool is not initialized for EventHubs: $name"
  }

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val pools = new MutableMap[String, ClientConnectionPool]()

  private def isInitialized(key: String): Boolean = pools.synchronized {
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

  /**
   * Looks up the connection pool instance associated with the connection
   * string in the provided [[EventHubsConf]] and retrieves an [[EventHubClient]]
   * for the borrower.
   *
   * @param ehConf the [[EventHubsConf]] used to lookup the connection pool
   * @return the borrowed [[EventHubsClient]]
   */
  def borrowClient(ehConf: EventHubsConf): EventHubClient = {
    pools.synchronized {
      if (!isInitialized(key(ehConf))) {
        pools.update(key(ehConf), new ClientConnectionPool(ehConf))
      }
    }

    val pool = get(key(ehConf))
    pool.borrowClient
  }

  /**
   * Looks up the connection pool instance associated with the connection
   * string the the provided [[EventHubsConf]] and returns the borrowed
   * [[EventHubsClient]] to the pool.
   *
   * @param ehConf the [[EventHubsConf]] use to lookup the connection pool
   * @param client the [[EventHubsClient]] being returned
   */
  def returnClient(ehConf: EventHubsConf, client: EventHubClient): Unit = {
    ensureInitialized(key(ehConf))
    val pool = get(key(ehConf))
    pool.returnClient(client)
  }
}

/**
 * Cache for [[ScheduledExecutorService]]s.
 */
object ClientThreadPool {
  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val pools = new MutableMap[String, ScheduledExecutorService]()

  private def key(ehConf: EventHubsConf): String = {
    ehConf.connectionString.toLowerCase
  }

  def get(ehConf: EventHubsConf): ScheduledExecutorService = {
    pools.synchronized {
      val keyName = key(ehConf)
      pools.getOrElseUpdate(
        keyName,
        Executors.newScheduledThreadPool(ehConf.threadPoolSize.getOrElse(DefaultThreadPoolSize)))
    }
  }
}
