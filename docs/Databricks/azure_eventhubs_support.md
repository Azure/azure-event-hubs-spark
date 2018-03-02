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

#### Azure Portal

When you get the connection string from the Azure Portal, it may or may not have the `EntityPath` key. Consider:

```scala
// Without an entity path
val without = "Endpoint=ENDPOINT;SharedAccessKeyName=KEY_NAME;SharedAccessKey=KEY"
    
// With an entity path 
val with = "Endpoint=sb://SAMPLE;SharedAccessKeyName=KEY_NAME;SharedAccessKey=KEY;EntityPath=EVENTHUB_NAME"
```

To connect to your EventHubs, an `EntityPath` must be present. If your connection string doesn't have one, don't worry!
This will take care of it:

```scala 
import org.apache.spark.eventhubs.ConnectionStringBuilder

val connectionString = ConnectionStringBuilder(without)   // defined in the previous code block
  .setEventHubName("EVENTHUB_NAME")
  .build
```

#### ConnectionStringBuilder

Alternatively, you can use the `ConnectionStringBuilder` to make your connection string.

```scala
import org.apache.spark.eventhubs.ConnectionStringBuilder

val connectionString = ConnectionStringBuilder()
  .setNamespaceName("NAMESPACE_NAME")
  .setEventHubName("EVENTHUB_NAME")
  .setSasKeyName("KEY_NAME")
  .setSasKey("KEY")
  .build
```

### EventHubsConf

All configuration relating to Event Hubs happens in your `EventHubsConf`. To create an `EventHubsConf`, you must
pass a connection string:

```scala
val connectionString = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(connectionString)
```

Please read the [Connection String](#connection-string) subsection for more information on obtaining a valid
connection string. 

Additionally, the following configurations are optional:

| Option | value | default | query type | meaning |
| ------ | ----- | ------- | ---------- | ------- |
| consumerGroup | `String` | "$Default" | streaming and batch | A consuer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) | 
| startingPositions | `Map[NameAndPartition, EventPosition]` | start of stream | streaming and batch | Sets starting positions for specific partitions. If any positions are set in this option, they take priority over any other option. If nothing is configured within this option, then the setting in `startingPosition` is used. If no position has been set in either option, we will start consuming from the beginning of the partition. |
| startingPosition | `EventPosition` | start of stream | streaming and batch | The starting position for your Structured Streaming job. Please read `startingPositions` for detail on which order the options are read. |
| endingPositions | `Map[NameAndPartition, EventPosition]` | end of stream | batch query | The ending position of a batch query on a per partition basis. This works the same as `startingPositions`. |
| endingPosition | `EventPosition` | end of stream | batch query | The ending position of a batch query. This workds the same as `startingPosition`.  | 
| maxEventsPerTrigger | `long` | `partitionCount * 2000` | streaming query | Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. | 
| receiverTimeout | `java.time.Duration` | 60 seconds | streaming and batch | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| operationTimeout | `java.time.Duration` | 60 seconds | streaming and batch | The amount of time Event Hub API calls will be retried before throwing an exception. |

For each option, there exists a corresponding setter in the EventHubsConf. For example:

```scala
import org.apache.spark.eventhubs._

val cs = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(cs)
  .setConsumerGroup("sample-cg")
  .setMaxEventsPerTrigger(10000)
  .setReceiverTimeout(Duration.ofSeconds(30))
```

#### EventPosition

The `EventHubsConf` allows users to specify starting (and ending) positions with the `EventPosition` class. `EventPosition` 
defines a position of an event in an Event Hub partition. The position can be an enqueued time, offset, sequence number,
the start of the stream, or the end of the stream. It's (hopefully!) pretty straightforward:

```scala
import org.apache.spark.eventhubs._

EventPosition.fromOffset("246812")          // Specifies offset 246812
EventPosition.fromSequenceNumber(100L)      // Specifies sequence number 100
EventPosition.fromEnqueuedTime(Instant.now) // Specifies any event after the current time 
EventPosition.fromStartOfStream             // Specifies from start of stream
EventPosition.fromEndOfStream               // Specifies from end of stream
```

If you'd like to start (or end) at a specific position, simply create the correct `EventPosition` and
set it in your `EventHubsConf`:

```scala
val cs = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(cs)
  .setStartingPosition(EventPosition.fromEndOfStream)
```

#### Per Partition Configuration

For advanced users, we have provided the option to configure starting and ending positions on a per partition
basis. Simply pass a `Map[NameAndPartition, EventPosition]` to your `EventHubsConf`. Consider:

```scala
// name is the EventHub name!
val positions = Map(
  NameAndPartition(name, 0) -> EventPosition.fromStartOfStream,
  NameAndPartition(name, 1) -> EventPosition.fromSequenceNumber(100L)
)

val cs = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(cs)
  .setStartingPositions(positions)
  .setStartingPosition(EventPosition.fromEndOfStream)
```

In this case, partition 0 starts from the beginning of the partition, partition 1 starts from sequence number `100L`, 
and all other partitions will start from the end of the partitions. You can start from any position on any partition
you'd like! 

#### IoT Hub

If using IoT Hub, getting your connection string is the only part of the process that is different - all 
other documentation still applies. Follow these instructions to get your EventHubs-compatible connection string: 

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
val ehConfWrite = EventHubsConf(connString)

val query =
  streamingSelectDF
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .options(ehConfWrite.toMap)
    .trigger(ProcessingTime("25 seconds"))
    .start()
```
