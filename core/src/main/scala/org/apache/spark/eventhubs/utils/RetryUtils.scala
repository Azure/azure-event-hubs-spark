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

package org.apache.spark.eventhubs.utils

import java.util.concurrent._

import com.microsoft.azure.eventhubs.EventHubException
import org.apache.spark.internal.Logging

import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ ExecutionContext, Future, Promise }

// TODO tests

/**
 * A utility object which makes retrying calls easier. There are methods
 * to retryJava synchronous and asynchronous calls.
 */
private[spark] object RetryUtils extends Logging {
  import org.apache.spark.eventhubs._

  val scheduled: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  /**
   * Delays the execution of a Future by a certain duration. This is
   * accomplished by using the [[[ScheduledExecutorService]] and
   * [[Promise]]s. In short, a [[Promise]] is created which completes with
   * the [[Future]] passed by the user. The [[Promise]] completes in a
   * thread within the [[ScheduledExecutorService]]. The task which
   * completes the [[Promise]] is scheduled to run after some delay
   * passed by the user.
   *
   * @param duration the delay before the user's [[Future]] is executed
   *                 on the [[ExecutionContext]]
   * @param value the operation which creates a [[Future]]
   * @param ec the [[ExecutionContext]]
   * @tparam T the type returned by the [[Future]]
   * @return the [[Future ]] created after the delay
   */
  final def after[T](duration: FiniteDuration)(value: => Future[T])(
      implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()
    val d = new Callable[Any] {
      override def call(): Unit = {
        val task = new Runnable { def run() = p completeWith value }
        ec.execute(task)
      }
    }
    scheduled.schedule(d, duration.toMillis, TimeUnit.MILLISECONDS)
    p.future
  }

  /**
   * This retries an operation which returns a [[CompletableFuture]].
   * The [[CompletableFuture]] is always converted to a Scala [[Future]].
   * The retryJava happens after a fixed delay.
   *
   * Only retires if the exception received on failure is an
   * [[EventHubException]] where isTransient is true.
   *
   * @param fn the function which creates the [[CompletableFuture]]
   * @param opName the name of the operation. This is to assist with logging.
   * @param maxRetry The number of times the operation will be retried.
   * @param delay The delay (in milliseconds) before the Future is run again.
   * @tparam T the result type from the [[CompletableFuture]]
   * @return the [[Future]] returned by the async operation
   */
  final def retryJava[T](fn: => CompletableFuture[T],
                         opName: String,
                         maxRetry: Int = RetryCount,
                         delay: Int = 10): Future[T] = {
    retryScala(toScala(fn), opName, maxRetry, delay)
  }

  /**
   * This retries an operation which returns a [[Future]].
   * The retryJava happens after a fixed delay.
   *
   * Only retires if the exception received on failure is an
   * [[EventHubException]] where isTransient is true.
   *
   * @param fn the function which creates the [[Future]]
   * @param opName the name of the operation. This is to assist with logging.
   * @param maxRetry The number of times the operation will be retried.
   * @param delay The delay (in milliseconds) before the Future is run again.
   * @tparam T the result type from the [[Future]]
   * @return the [[Future]] returned by the async operation
   */
  final def retryScala[T](fn: => Future[T],
                          opName: String,
                          maxRetry: Int = RetryCount,
                          delay: Int = 10): Future[T] = {
    def retryHelper(fn: => Future[T], retryCount: Int): Future[T] = {
      val taskId = EventHubsUtils.getTaskId
      fn.recoverWith {
        case eh: EventHubException if eh.getIsTransient =>
          if (retryCount >= maxRetry) {
            logInfo(s"(TID $taskId) failure: $opName")
            throw eh
          }
          logInfo(s"(TID $taskId) retrying $opName after $delay ms")
          after(delay.milliseconds)(retryHelper(fn, retryCount + 1))
        case t: Throwable =>
          t.getCause match {
            case eh: EventHubException if eh.getIsTransient =>
              if (retryCount >= maxRetry) {
                logInfo(s"(TID $taskId) failure: $opName")
                throw eh
              }
              logInfo(s"(TID $taskId) retrying $opName after $delay ms")
              after(delay.milliseconds)(retryHelper(fn, retryCount + 1))
            case _ =>
              logInfo(s"(TID $taskId) failure: $opName")
              throw t
          }
      }
    }
    retryHelper(fn, 0)
  }

  /**
   * The same retry policy as [[retryJava()]] and [[retryScala()]]
   * is used here. There is an additional constraint: the Future
   * must complete with a *not* null result. If the result is
   * null, we'll redo the operation until we get a *not* null
   * result.
   *
   * @param fn the operation to be retried
   * @param opName the name of the operation. Used for logging.
   * @tparam T the type returned by the [[Future]]
   * @return the [[Future]] from the provided async operation.
   */
  def retryNotNull[T](fn: => CompletableFuture[T], opName: String): Future[T] = {
    retryJava(fn, opName).flatMap { result =>
      if (result == null) {
        retryNotNull(fn, opName)
      } else {
        Future.successful(result)
      }
    }
  }
}
