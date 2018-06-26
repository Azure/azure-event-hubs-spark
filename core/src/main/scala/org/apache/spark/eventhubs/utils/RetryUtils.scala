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
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

// TODO tests

/**
 * A utility object which makes retrying calls easier. There are methods
 * to retry synchronous and asynchronous calls.
 */
private[spark] object RetryUtils extends Logging {
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
   * The retries are done indefinitely with exponential backoff.
   *
   * Only retires if the exception received on failure is an
   * [[EventHubException]] where isTransient is true.
   *
   * @param fn the function which creates the [[CompletableFuture]]
   * @param opName the name of the operation. This is to assit with logging.
   * @param factor the factor by which the wait time scales.
   * @param init initial wait time in milliseconds
   * @param cur the current wait time (in milliseconds) before a retry is attempted
   * @tparam T the result type from the [[CompletableFuture]]
   * @return the [[Future]] returned by the async operation
   */
  final def retry[T](fn: => CompletableFuture[T],
                     opName: String,
                     factor: Float = 1.5f,
                     init: Int = 1,
                     cur: Int = 0): Future[T] = {
    toScala(fn).recoverWith {
      case eh: EventHubException if eh.getIsTransient =>
        val delay = if (cur == 0) {
          init
        } else {
          Math.ceil(cur * factor).toInt
        }
        logInfo(s"retrying $opName after $delay")
        after(delay.milliseconds)(retry(fn, opName, factor, init, delay))
      case t: Throwable => throw t
    }
  }

  /**
   * Retries synchronous calls for a fixed number of attempts.
   *
   * Only retries if the exception thrown on failure is an
   * [[EventHubException]] where isTransient is marked as true.
   *
   * @param n the number of retries that will be performed.
   * @param method the method name. This is to help with logging.
   * @param fn the operation to be retried
   * @tparam T the result type of the passed operation
   * @return the result of the passed operation
   */
  @annotation.tailrec
  final def retry[T](n: Int)(method: String)(fn: => T): T = {
    logInfo(s"retry: $method: attempts left: $n")
    Try { fn } match {
      case Success(x) => x
      case Failure(e: EventHubException) if e.getIsTransient && n > 1 =>
        logInfo(s"retry: $method failure.", e)
        retry(n - 1)(method)(fn)
      case Failure(e) => throw e
    }
  }
}
