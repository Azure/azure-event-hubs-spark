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

import java.time.Duration

import org.scalatest.FunSuite

class ConnectionStringBuilderSuite extends FunSuite {

  import ConnectionStringBuilderSuite._

  private val validateConnStrBuilder = (connStrBuilder: ConnectionStringBuilder) => {
    assert(connStrBuilder.getEventHubName == CorrectEntityPath)
    assert(connStrBuilder.getEndpoint.getHost == CorrectEndpoint)
    assert(connStrBuilder.getSasKey == CorrectKey)
    assert(connStrBuilder.getSasKeyName == CorrectKeyName)
    assert(connStrBuilder.getOperationTimeout == CorrectOperationTimeout)
  }

  test("parse invalid connection string") {
    intercept[Exception] {
      new ConnectionStringBuilder("something")
    }
  }

  test("throw on unrecognized parts") {
    intercept[Exception] {
      new ConnectionStringBuilder(correctConnectionString + ";" + "something")
    }
  }

  test("parse valid connection string") {
    val connStrBuilder = new ConnectionStringBuilder(correctConnectionString)
    validateConnStrBuilder(connStrBuilder)
  }

  test("exchange connection string across constructors") {
    val connStrBuilder = new ConnectionStringBuilder(correctConnectionString)
    val secondConnStr = new ConnectionStringBuilder()
      .setEndpoint(connStrBuilder.getEndpoint)
      .setEventHubName(connStrBuilder.getEventHubName)
      .setSasKeyName(connStrBuilder.getSasKeyName)
      .setSasKey(connStrBuilder.getSasKey)
    secondConnStr.setOperationTimeout(connStrBuilder.getOperationTimeout)
    validateConnStrBuilder(new ConnectionStringBuilder(secondConnStr.toString))
  }

  test("property setters") {
    val connStrBuilder = new ConnectionStringBuilder(correctConnectionString)
    val testConnStrBuilder = new ConnectionStringBuilder(connStrBuilder.toString)
    validateConnStrBuilder(testConnStrBuilder)
    connStrBuilder.setOperationTimeout(Duration.ofSeconds(8))
    val testConnStrBuilder1 = new ConnectionStringBuilder(connStrBuilder.toString)
    assert(testConnStrBuilder1.getOperationTimeout.getSeconds == 8)
  }
}

object ConnectionStringBuilderSuite {
  private val CorrectEndpoint = "endpoint1"
  private val CorrectEntityPath = "eventhub1"
  private val CorrectKeyName = "somekeyname"
  private val CorrectKey = "somekey"
  private val CorrectOperationTimeout = Duration.ofSeconds(5)

  private val correctConnectionString =
    s"Endpoint=sb://$CorrectEndpoint;EntityPath=$CorrectEntityPath;SharedAccessKeyName=$CorrectKeyName;" +
      s"SharedAccessKey=$CorrectKey;OperationTimeout=$CorrectOperationTimeout;"
}
