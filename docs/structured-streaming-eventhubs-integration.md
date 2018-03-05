# Structured Streaming + Azure Event Hubs Integration Guide 

Structured Streaming integration for Azure Event Hubs to read data from Event Hubs. 

## Linking
For Scala/Java applications using SBT/Maven project defnitions, link your application with the following artifact:

```
  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.11
  version = 2.3.0
```

For Python applications, you need to add this above library and its dependencies when deploying your applciation.
See the [Deploying](#deploying) subsection below.

## User Configuration

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
| consumerGroup | `String` | "$Default" | streaming and batch | A consumer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) | 
| startingPositions | `Map[NameAndPartition, EventPosition]` | start of stream | streaming and batch | Sets starting positions for specific partitions. If any positions are set in this option, they take priority over any other option. If nothing is configured within this option, then the setting in `startingPosition` is used. If no position has been set in either option, we will start consuming from the beginning of the partition. |
| startingPosition | `EventPosition` | start of stream | streaming and batch | The starting position for your Structured Streaming job. Please read `startingPositions` for detail on which order the options are read. |
| endingPositions | `Map[NameAndPartition, EventPosition]` | end of stream | batch query | The ending position of a batch query on a per partition basis. This works the same as `startingPositions`. |
| endingPosition | `EventPosition` | end of stream | batch query | The ending position of a batch query. This workds the same as `startingPosition`.  | 
| maxEventsPerTrigger | `long` | `partitionCount * 1000` | streaming query | Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. | 
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

## Reading Data from Event Hubs

### Creating an Event Hubs Source for Streaming Queries 

```scala
// Source with default settings
val connectionString = "Valid EventHubs connection string."
val ehConf = EventHubsConf(connectionString)

val df = spark
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

// Source with per partition starting positions and rate limiting. In this case, we'll start from 
// a sequence number for partition 0, enqueued time for partition 3, the end of stream
// for partition 5, and the start of stream for all other partitions.
val connectionString = "Valid EventHubs connection string."

val positions = Map(
  0 -> EventPosition.fromSequenceNumber(1000L, isInclusive = true),
  3 -> EventPosition.fromEnqueuedTime(Instant.now),
  5 -> EventPosition.fromEndOfStream
)

val ehConf = EventHubsConf(connectionString)
  .setStartingPositions(positions)
  .setMaxEventsPerTrigger(10000)

val df = spark
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()
```

### Creating an Event Hubs Source for Batch Queries 

```scala
// Simple batch query
val df = spark
  .read
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()
df.select("body").as[String]

// start from same place across all partitions. end at the same place accross all partitions.
val ehConf = EventHubsConf("VALID.CONNECTION.STRING")
  .setStartingPosition(EventPosition.fromSequenceNumber(1000L)
  .setEndingPosition(EventPosition.fromEnqueuedTime(Instant.now)

// per partition config
val start = Map(
  0 -> EventPosition.fromSequenceNumber(1000L),
  1 -> EventPosition.fromOffset("100")
)

val end = Map(
  0 -> EventPosition.fromEnqueuedTime(Instant.now),
  1 -> EventPosition.fromSequenceNumber(1000L)
)

val ehConf = EventHubsConf("VALID.CONNECTION.STRING")
  .setStartingPositions(start)
  .setEndingPositions(end)
```


Each row in the source has the following schema:

| Column | Type |
| ------ | ---- |
| body | string |
| offset | long |
| sequenceNumber | long |
| enqueuedTime | timestamp |
| publisher | string |
| partitionKey | string | 

## Writing Data to EventHubs

Here, we describe the support for writting Streaming Queries and Batch Queries to Azure EventHubs. Take note that, today, Azure EventHubs only supports
at least once semantics. Consequently, when writing - either Streaming Queries or Batch Queries - to  EventHubs, some records may be duplicated;
this can happen, for example, if EventHubs needs to retry an event that was not acknowledged by the EventHubs service, event if the service received and 
stored the event. Structured Streaming cannot prevent such duplicates from ocurring due to these EventHubs write semantics. However, if writing the query 
is successful, then you can assume that the query output was written at least once. A possible solution to remove duplicates when reading the written data
could be to introduce a primary (unique) key that can be used to perform de-duplication when reading. 

The Dataframe being written to EventHubs should have the following columns in the schema:

| Column | Type | 
| ------ | ---- | 
| body (required) | string or binary |
| partitionId (*optional) | string |
| partitionKey (*optional) | string |

* Only one (partitionId or partitionKey) can be set at a time. If both are set, your Structured Streaming job will be stopped. 

The body column is the only required option. If a partitionId and partitionKey are not provided, then events will distributed to partitions
using a round-robin model. Alternatively, if a partitionId is provided, the query output will be sent to that specific partition exclusively. 
Sending to a single partition is *not* a recommended pattern. Finally, if a partionKey is provided, each event will be sent with the
provided partitionKey. For more information on how a partitionKey works, click 
[here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-programming-guide#partition-key).

### Creating an EventHubs Sink for Streaming Queries 

```scala
// Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
val ds = df
  .select("body")
  .writeStream
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // A new EventHubsConf that contains the correct connection string for the destination EventHub.
  .start()
  
// Write body data from a DataFrame to EventHubs with a partitionKey
val ds = df
  .selectExpr("partitionKey", "body")
  .format("eventHubs")
  .options(ehWriteConf.toMap)    // A new EventHubsConf that contains the correct connection string for the destination EventHub.
  .start()
```

### Writing the output of Batch Queries to EventHubs

```scala
// Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
df.select("body")
  .write
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // A new EventHubsConf that contains the correct connection string for the destination EventHub.
  .save()
  
// Write body data   
df.selectExpr("partitionKey", "body")
  .write
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // A new EventHubsConf that contains the correct connection string for the destination EventHub.  
  .save()
```

## Deploying 

As with any Spark applications, `spark-submit` is used to launch your application. `azure-eventhubs-spark_2.11`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.0 ...

For experimenting on `spark-shell`, you can also use `--packages` to add `azure-eventhubs-spark_2.11` and its dependencies directly,

    ./bin/spark-shell --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.0 ...

See [Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html) for more details about submitting
applications with external dependencies.
