# Azure Event Hubs Support

## Requirements 

The Azure Event Hubs connector for Structured Streaming is compatible with Databricks Runtime Version 3.5 and 
Databricks Runtime Version 4.0. Currently, the Azure Event Hubs connector is not pre-packaged with Spark or Databricks. 
You can create a library in Databricks with the following Maven coordinate:

`com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.0`

## Quickstart

Let's start with a quick example: WordCount. The following notebook is all that it takes to run WordCount using Structured Streaming
with Azure Event Hubs. 

### Event Hubs WordCount with Structured Streaming

> Note: I have this in a notebook. How can we embed this?  

```scala
// This is a WordCount example with the following:
//     - Event Hubs as a Structured Streaming Source
//     - Stateful operation (groupBy) to calculate running counts
//
// Requirements: Databricks version 3.5 or 4.0 and beyond. 

import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition }

// Setup EventHubs configurations
val connectionString = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.StartOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

// split lines by whitespaces and explode the array as rows of 'word'
val df = eventhubs.select(explode(split($"body", "\\s+")).as("word"))
  .groupBy("$"word")
  .count

// follow the word counts as it updates
display(df.select($"word", $"count"))

// Try it yourself
// The Event Hubs Source also includes the ingestion timestamp of records. Try counting the words by the ingestion time window as well. 
```

## Schema 

The schema of the records are:

| Column         | Type      |
| ------         | ----      |
| body           | string    |
| offset         | long      |
| sequenceNumber | long      |
| enqueuedTime   | timestamp |
| publisher      | string    |
| partitionKey   | string    | 

The `body` is always provided as a `string` with UTF-8 encoding. 

## Configuration

> Note: As Structured Streaming is still under heavy development, this list may not be up to date. 

### Connection String
An Event Hubs connection string is required to connect to the Event Hubs service. You can get the connection string 
for your Event Hubs instance from the [Azure Portal](https://portal.azure.com) or by using the `ConnectionStringBuilder` 
in our library. 

#### IoT Hub
If using IoT Hub, follow these instructions to get your EventHubs-compatible connection string:

    1. Go to the [Azure Portal](https://ms.portal.azure.com) and find your IoT Hub instance
    2. Click on **Endpoints** under **Messaging**. Then click on **Events**.
    3. Find your ```EventHub-compatible name``` and ```EventHub-compatible endpoint```.

```scala
import org.apache.spark.eventhubs.ConnectionStringBuilder

// Build connection string with the above information 
val connectionString = ConnectionStringBuilder("YOUR.EVENTHUB.COMPATIBLE.ENDPOINT")
  .setEventHubName("YOUR.EVENTHUB.COMPATIBLE.NAME")
  .build
```

### EventPosition
The `EventPosition` defines a position of an event in an event hub partition. The position can be an enqueued time, offset,
or sequence number. If you would like start from a specific point in a partition, you must pass an `EventPosition` to 
your [`EventHubsConf`](#eventhubsconf). Consider the following example:

```scala
val seqNo = EventPosition.fromSequenceNumber(400L, isInclusive = true)
```

This creates an `EventPosition` which specifies the sequence number 400. If `isInclusive` was false, the position would 
be sequence number 401. The following options are available for specificying positions:

| Method Name        | Required Parameter   | Optional Parameters | Returns | 
| -----------        | ------------------   | ------------------- | ------- |
| fromSequenceNumber | sequenceNumber: `Long` | isInclusive = false | `EventPosition` at the given sequence number. |
| fromOffset         | offset: `String`       | isInclusive = false | `EventPosition` at the given byte offset. |
| fromEnqueuedTime   | enqueuedTime: `java.time.Instant` | None | `EventPosition` at the given enqueued time. |
| fromStartOfStream  | None | None | `EventPosition` for the start of the stream. |
| fromEndOfStream    | None | None | `EventPosition` for the end of the stream. | 

### EventHubsConf
All configuration relating to Event Hubs happens in your `EventHubsConf`. A connection string must be passed to the EventHubsConf constructor like so:

```scala
val connectionString = "ValidEventHubsConnectionString"
val ehConf = EventHubsConf(connectionString)
```

Additionally, the follow configurations are optional:

| Option | value | default | query type | meaning |
| ------ | ----- | ------- | ---------- | ------- |
| consumerGroup | `String` | "$Default" | N/A | A consuer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) | 
| startingPositions | `Map[PartitionId, EventPosition]` | start of stream | N/A | Starting positions for specific partitions. If any positions are set in this option, they take priority when starting the Structured Streaming job. If nothing is configured for a specific partition, then the `EventPosition` set in startingPosition is used. If no position set there, we will start consuming from the beginning of the partition. |
| startingPosition | `EventPosition` | start of stream | N/A | The starting position for your Structured Streaming job. If a specific EventPosition is *not* set for a partition using startingPositions, then we use the `EventPosition` set in startingPosition. If nothing is set in either option, we will begin consuming from the beginning of the partition. |
| failOnDataLoss | `true` or `false` | true | N/A | Whether to fail the query when it's possible that data is lost (e.g. when sequence numbers are out of range). This may be a false alarm. You can disable it when it doesn't work as you expected. | 
| maxEventsPerTrigger | `long` | none | N/A | Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. | 
| receiverTimeout | `java.time.Duration` | 60 seconds | N/A | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| operationTimeout | `java.time.Duration` | 60 seconds | N/A | The amount of time Event Hub API calls will be retried before throwing an exception. |

### Production Structured Streaming with Azure Event Hubs

> Note: how do we embed the notebook like it is on the databricks site? 

```scala
// Databricks notebook source
// MAGIC %md ## Setup Connection to Azure Event Hubs

// COMMAND ----------

import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition }

val connectionString = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.endOfStream)
  .setFailOnDataLoss(true)
  .toMap

var streamingInputDF = 
  spark.readStream
    .format("eventhubs")
    .options(ehConf)
    .load()

// COMMAND ----------

// MAGIC %md ## streamingInputDF.printSchema
// MAGIC 
// MAGIC   root <br><pre>
// MAGIC    </t>|-- body: string (nullable = true) <br>
// MAGIC    </t>|-- offset: long (nullable = true) <br>
// MAGIC    </t>|-- sequenceNumber: long (nullable = true) <br>
// MAGIC    </t>|-- enqueuedTime: timestamp (nullable = true) <br>
// MAGIC    </t>|-- publisher: string (nullable = true) <br>
// MAGIC    </t>|-- partitionKey: string (nullable = true) <br>

// COMMAND ----------

// MAGIC %md ## Sample Body
// MAGIC <pre>
// MAGIC {
// MAGIC </t>"city": "<CITY>", 
// MAGIC </t>"country": "United States", 
// MAGIC </t>"countryCode": "US", 
// MAGIC </t>"isp": "<ISP>", 
// MAGIC </t>"lat": 0.00, "lon": 0.00, 
// MAGIC </t>"query": "<IP>", 
// MAGIC </t>"region": "CA", 
// MAGIC </t>"regionName": "California", 
// MAGIC </t>"status": "success", 
// MAGIC </t>"hittime": "2017-02-08T17:37:55-05:00", 
// MAGIC </t>"zip": "38917" 
// MAGIC }

// COMMAND ----------

// MAGIC %md ## GroupBy, Count

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"body"), "$.zip").alias("zip"))
    .groupBy($"zip") 
    .count()

// COMMAND ----------

// MAGIC %md ## Window

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"body"), "$.zip").alias("zip"), get_json_object(($"body"), "$.hittime").alias("hittime"))
   .groupBy($"zip", window($"hittime".cast("timestamp"), "10 minute", "5 minute", "2 minute"))
   .count()


// COMMAND ----------

// MAGIC %md ## Memory Output

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("memory")        
    .queryName("isphits")     
    .outputMode("complete") 
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## Console Output

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("console")        
    .outputMode("complete") 
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## File Output with Partitions

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"body"), "$.zip").alias("zip"),    get_json_object(($"body"), "$.hittime").alias("hittime"), date_format(get_json_object(($"body"), "$.hittime"), "dd.MM.yyyy").alias("day"))
    .groupBy($"zip") 
    .count()
    .as[(String, String)]

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("parquet")
    .option("path", "/mnt/sample/test-data")
    .option("checkpointLocation", "/mnt/sample/check")
    .partitionBy("zip", "day")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ##### Create Table

// COMMAND ----------

// MAGIC %sql CREATE EXTERNAL TABLE  test_par
// MAGIC     (hittime string)
// MAGIC     PARTITIONED BY (zip string, day string)
// MAGIC     STORED AS PARQUET
// MAGIC     LOCATION '/mnt/sample/test-data'

// COMMAND ----------

// MAGIC %md ## JDBC Sink

// COMMAND ----------

import java.sql._

class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, String)] {
      val driver = "com.mysql.jdbc.Driver"
      var connection:Connection = _
      var statement:Statement = _
      
    def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: (String, String)): Unit = {
        statement.executeUpdate("INSERT INTO zip_test " + 
                "VALUES (" + value._1 + "," + value._2 + ")")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close
      }
   }


// COMMAND ----------

val url="jdbc:mysql://<mysqlserver>:3306/test"
val user ="user"
val pwd = "pwd"

val writer = new JDBCSink(url,user, pwd)
val query =
  streamingSelectDF
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(ProcessingTime("25 seconds"))
    .start()
    
// COMMAND ---------- 

// MAGIC %md ## EventHubs Sink

// COMMAND ----------

val connString = "YOUR.CONNECTION.STRING"    // connection string for where you want to WRITE to. 
val ehConfWrite = new EventHubsConf(connString)

val query =
  streamingSelectDF
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .options(ehConfWrite.toMap)
    .trigger(ProcessingTime("25 seconds"))
    .start()
```
