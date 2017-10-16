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

object EventhubsArgumentParser {

  type ArgumentMap = Map[Symbol, Any]

  def usageExample(): Unit = {

    val eventhubsNamespace: String = "sparkstreamingeventhub-ns"
    val eventhubsName: String = "sparkstreamingeventhub"
    val policyName: String = "[EventhubsPolicyName]"
    val policyKey: String = "[EventhubsPolicyKey]"
    val consumerGroup: String = "$default"
    val partitionCount: Int = 32
    val batchInterval: Int = 10
    val checkpointDirectory: String = "/EventCheckpoint10"
    val eventCountFolder: String = "/EventCount/EventCount10"
    val eventStoreFolder: String = "/EventStore/EventStore10"
    val eventHiveTable: String = "EventHiveTable10"
    val sqlServerFQDN: String = "servername.database.windows.net"
    val sqlDatabaseName: String = "databasename"
    val databaseUsername: String = "[DatabaseUsername]"
    val databasePassword: String = "[DatabasePassword]"
    val eventSQLTable: String = "EventSQLTable10"
    val timeoutInMinutes: Long = -1

    println()
    println(s"Usage [EventhubsEventCount]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.examples.EventHubsEventCount" +
      s" /home/hdiuser/spark/SparkStreamingDataPersistence.jar --eventhubs-namespace \'$eventhubsNamespace\'" +
      s" --eventhubs-name \'$eventhubsName\' --policy-name \'$policyName\' --policy-key \'$policyKey\'" +
      s" --consumer-group \'$consumerGroup\' --partition-count  $partitionCount" +
      s" --batch-interval-in-seconds $batchInterval --checkpoint-directory \'$checkpointDirectory\'" +
      s" --event-count-folder \'$eventCountFolder\' --job-timeout-in-minutes $timeoutInMinutes")
    println()
    println(s"Usage [EventhubsToAzureBlobAsJSON]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.examples.EventHubsEventCount" +
      s" /home/hdiuser/spark/SparkStreamingDataPersistence.jar --eventhubs-namespace \'$eventhubsNamespace\'" +
      s" --eventhubs-name \'$eventhubsName\' --policy-name \'$policyName\' --policy-key \'$policyKey\'" +
      s" --consumer-group \'$consumerGroup\' --partition-count  $partitionCount" +
      s" --batch-interval-in-seconds $batchInterval --checkpoint-directory \'$checkpointDirectory\'" +
      s" --event-count-folder \'$eventCountFolder\' --event-store-folder \'$eventStoreFolder\'" +
      s" --job-timeout-in-minutes $timeoutInMinutes")
    println()
    println(s"Usage [EventhubsToHiveTable]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.examples.EventHubsEventCount" +
      s" /home/hdiuser/spark/SparkStreamingDataPersistence.jar --eventhubs-namespace \'$eventhubsNamespace\'" +
      s" --eventhubs-name \'$eventhubsName\' --policy-name \'$policyName\' --policy-key \'$policyKey\'" +
      s" --consumer-group \'$consumerGroup --partition-count  $partitionCount" +
      s" --batch-interval-in-seconds $batchInterval --checkpoint-directory \'$checkpointDirectory\'" +
      s" --event-count-folder \'$eventCountFolder\' --event-hive-table \'$eventHiveTable\'" +
      s" --job-timeout-in-minutes $timeoutInMinutes")
    println()
    println(s"Usage [EventhubsToSQLTable]: spark-submit --master yarn-cluster ..." +
      s" --class com.microsoft.spark.streaming.examples.EventHubsEventCount" +
      s" /home/hdiuser/spark/SparkStreamingDataPersistence.jar --eventhubs-namespace $eventhubsNamespace" +
      s" --eventhubs-name \'$eventhubsName\' --policy-name \'$policyName\' --policy-key \'$policyKey\'" +
      s" --consumer-group \'$consumerGroup\' --partition-count  $partitionCount" +
      s" --batch-interval-in-seconds $batchInterval --checkpoint-directory \'$checkpointDirectory\'" +
      s" --event-count-folder \'$eventCountFolder\' --sql-server-fqdn \'$sqlServerFQDN\'" +
      s" --sql-database-name \'$sqlDatabaseName\' --database-username \'$databaseUsername\'" +
      s" --database-password \'$databasePassword\' --event-sql-table \'$eventSQLTable\'" +
      s" --job-timeout-in-minutes $timeoutInMinutes")
    println()
  }

  def parseArguments(argumentMap : ArgumentMap, argumentList: List[String]) : ArgumentMap = {

    argumentList match {
      case Nil => argumentMap
      case "--eventhubs-namespace" :: value:: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventhubsNamespace) -> value.toString), tail)
      case "--eventhubs-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventhubsName) -> value.toString), tail)
      case "--policy-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.PolicyName) -> value.toString), tail)
      case "--policy-key" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.PolicyKey) -> value.toString), tail)
      case "--consumer-group" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.ConsumerGroup) -> value.toString), tail)
      case "--partition-count" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.PartitionCount) -> value.toInt), tail)
      case "--batch-interval-in-seconds" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds) -> value.toInt), tail)
      case "--checkpoint-directory" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.CheckpointDirectory) -> value.toString), tail)
      case "--event-count-folder" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventCountFolder) -> value.toString), tail)
      case "--event-store-folder" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventStoreFolder) -> value.toString), tail)
      case "--event-hive-table" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventHiveTable) -> value.toString), tail)
      case "--sql-server-fqdn" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.SQLServerFQDN) -> value.toString), tail)
      case "--sql-database-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.SQLDatabaseName) -> value.toString), tail)
      case "--database-username" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.DatabaseUsername) -> value.toString), tail)
      case "--database-password" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.DatabasePassword) -> value.toString), tail)
      case "--event-sql-table" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.EventSQLTable) -> value.toString), tail)
      case "--job-timeout-in-minutes"  :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(EventhubsArgumentKeys.TimeoutInMinutes) -> value.toLong), tail)
      case option :: tail =>
        println()
        println("Unknown option: " + option)
        println()
        usageExample()
        sys.exit(1)
    }
  }

  def verifyEventhubsEventCountArguments(argumentMap : ArgumentMap): Unit = {

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventhubsNamespace)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventhubsName)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.PolicyName)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.PolicyKey)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.ConsumerGroup)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.PartitionCount)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.CheckpointDirectory)))

    assert(argumentMap(Symbol(EventhubsArgumentKeys.PartitionCount)).asInstanceOf[Int] > 0)
    assert(argumentMap(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int] > 0)
  }

  def verifyEventhubsToAzureBlobAsJSONArguments(argumentMap : ArgumentMap): Unit = {

    verifyEventhubsEventCountArguments(argumentMap)

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventStoreFolder)))
  }

  def verifyEventhubsToHiveTableArguments(argumentMap : ArgumentMap): Unit = {

    verifyEventhubsEventCountArguments(argumentMap)

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventHiveTable)))
  }

  def verifyEventhubsToSQLTableArguments(argumentMap : ArgumentMap): Unit = {

    verifyEventhubsEventCountArguments(argumentMap)

    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.SQLServerFQDN)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.SQLDatabaseName)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.DatabaseUsername)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.DatabasePassword)))
    assert(argumentMap.contains(Symbol(EventhubsArgumentKeys.EventSQLTable)))
  }
}
