package org.apache.spark.eventhubs.client

import java.util.concurrent.{ ExecutorService, Executors }

/**
 * Thread pool for EventHub client. We create one thread pool per JVM.
 * Threads are created on demand if none are available.
 * In future releases, thread pool will be configurable by users.
 */
object ClientThreadPool {
  val pool: ExecutorService = Executors.newCachedThreadPool
}
