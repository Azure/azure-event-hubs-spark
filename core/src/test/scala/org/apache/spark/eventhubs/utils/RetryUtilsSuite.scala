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

import java.io.IOException
import java.util.concurrent.CompletableFuture

import com.microsoft.azure.eventhubs.EventHubException
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RetryUtilsSuite extends FunSuite with ScalaFutures {

  import RetryUtilsSuite._

  test("don't retry successful Future") {
    val tries = incrementFutureIterator(1)
    val result = RetryUtils.retryScala(tries.next, "test", maxRetry = 3, delay = 1).futureValue
    assert(1 === result)
  }

  test("don't retry failed Future with normal exception") {
    val fails = Iterator(Future.failed(new IOException("not retry")))
    val tries = fails ++ incrementFutureIterator(1)
    val exception =
      RetryUtils.retryScala(tries.next, "test", maxRetry = 3, delay = 1).failed.futureValue
    assert("not retry" === exception.getMessage)
  }

  test("don't retry failed Future with non-transient EventHubException") {
    val tries = Iterator(nonTransientEHE()) ++ incrementFutureIterator(1)
    val exception =
      RetryUtils.retryScala(tries.next, "test", maxRetry = 3, delay = 1).failed.futureValue
    assert("nonTransient" === exception.getMessage)
  }

  test("retry maxRetry times until success") {
    val fails = Iterator(failedWithEHE(), causedByEHE(), failedWithEHE())
    val tries = fails ++ incrementFutureIterator(4)

    val result = RetryUtils.retryScala(tries.next, "test", maxRetry = 3, delay = 1).futureValue
    assert(4 === result)
  }

  test("retry maxRetry times until failure") {
    val fails = Iterator(failedWithEHE(), causedByEHE(), failedWithEHE(), causedByEHE())
    val tries = fails ++ incrementFutureIterator(4)

    val exception =
      RetryUtils.retryScala(tries.next, "test", maxRetry = 3, delay = 1).failed.futureValue
    assert("causedBy" === exception.getMessage)
  }

  test("retryNotNull") {
    val nullFuture: CompletableFuture[AnyRef] =
      CompletableFuture.completedFuture(null.asInstanceOf[AnyRef])
    val normalFuture: CompletableFuture[Int] =
      CompletableFuture.completedFuture(10)

    val tries = Iterator.continually(nullFuture).take(9) ++ Iterator(normalFuture)
    val result = RetryUtils.retryNotNull(tries.next, "test").futureValue
    assert(10 === result)
  }
}

object RetryUtilsSuite {
  def failedWithEHE(): Future[Int] = Future.failed(new EventHubException(true, "failedWith"))

  def causedByEHE(): Future[Int] = {
    val causedBy = new EventHubException(true, "causedBy")
    Future.failed(new IOException(causedBy))
  }

  def nonTransientEHE(): Future[Int] = Future.failed(new EventHubException(false, "nonTransient"))

  def incrementFutureIterator(value: Int = 0): Iterator[Future[Int]] =
    Iterator.from(value).map(Future(_))
}
