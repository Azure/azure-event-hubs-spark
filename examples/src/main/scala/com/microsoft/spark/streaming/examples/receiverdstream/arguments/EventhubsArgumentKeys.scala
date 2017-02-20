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

package com.microsoft.spark.streaming.examples.receiverdstream.arguments

object EventhubsArgumentKeys extends Enumeration {
  val EventhubsNamespace: String = "eventhubsNamespace"
  val EventhubsName: String = "eventhubsName"
  val PolicyName: String = "policyName"
  val PolicyKey: String = "policyKey"
  val ConsumerGroup: String = "consumerGroup"
  val PartitionCount: String = "partitionCount"
  val BatchIntervalInSeconds: String = "batchInterval"
  val CheckpointDirectory: String = "checkpointDirectory"
  val EventCountFolder: String = "eventCountFolder"
  val EventStoreFolder: String = "eventStoreFolder"
  val EventHiveTable: String = "eventHiveTable"
  val SQLServerFQDN: String = "sqlServerFQDN"
  val SQLDatabaseName: String = "sqlDatabaseName"
  val DatabaseUsername: String = "databaseUsername"
  val DatabasePassword: String = "databasePassword"
  val EventSQLTable: String = "eventSQLTable"
  val TimeoutInMinutes: String = "jobTimeout"
}
