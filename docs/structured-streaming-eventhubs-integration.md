# Structured Streaming + Event Hubs Integration Guide 

Structured Streaming integration for Azure Event Hubs to read data from Event Hubs. 

## Table of Contents
* [Linking](#linking)
* [User Configurations](#user-configuration)
  * [Connection String](#connection-string)
  * [EventHubsConf](#eventhubsconf)
* [Reading Data from Event Hubs](#reading-data-from-event-hubs)
  * [Creating an Event Hubs Source for Streaming Queries](#creating-an-event-hubs-source-for-streaming-queries)
  * [Creating an Event Hubs Source for Batch Queries](#creating-an-event-hubs-source-for-batch-queries)
* [Writing Data to EventHubs](#writing-data-to-eventhubs)
  * [Creating an EventHubs Sink for Streaming Queries](#creating-an-eventhubs-sink-for-streaming-queries)
  * [Writing the output of Batch Queries to EventHubs](#writing-the-output-of-batch-queries-to-eventhubs)
* [Managing Throughput](#managing-throughput)
* [Recovering from Failures with Checkpointing](#recovering-from-failures-with-checkpointing)
* [Serialization of Event Data Properties](#serialization-of-event-data-properties)
* [Deploying](#deploying)

## Linking
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

```
  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.11
  version = 2.3.21

or

  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.12
  version = 2.3.21
```

For Python applications, you need to add this above library and its dependencies when deploying your application.
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
val withEntity = "Endpoint=sb://SAMPLE;SharedAccessKeyName=KEY_NAME;SharedAccessKey=KEY;EntityPath=EVENTHUB_NAME"
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
| consumerGroup | `String` | "$Default" | streaming and batch | A consumer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers). | 
| startingPositions | `Map[NameAndPartition, EventPosition]` | end of stream | streaming and batch | Sets starting positions for specific partitions. If any positions are set in this option, they take priority over any other option. If nothing is configured within this option, then the setting in `startingPosition` is used. If no position has been set in either option, we will start consuming from the end of the partition. |
| startingPosition | `EventPosition` | end of stream | streaming and batch | The starting position for your Structured Streaming job. If a specific EventPosition is *not* set for a partition using `startingPositions`, then we use the `EventPosition` set in `startingPosition`. If nothing is set in either option, we will begin consuming from the end of the partition. |
| endingPositions | `Map[NameAndPartition, EventPosition]` | end of stream | batch query | The ending position of a batch query on a per partition basis. This works the same as `startingPositions`. |
| endingPosition | `EventPosition` | end of stream | batch query | The ending position of a batch query. This works the same as `startingPosition`.  | 
| maxEventsPerTrigger | `long` | `partitionCount * 1000` | streaming query | Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. | 
| receiverTimeout | `java.time.Duration` | 60 seconds | streaming and batch | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| operationTimeout | `java.time.Duration` | 300 seconds | streaming and batch | The amount of time Event Hub API calls will be retried before throwing an exception. |
| prefetchCount | `int` | `500` | streaming and batch | Sets the prefetch count for the underlying receiver and controls how many events are received in advance. The acceptable range for prefetch count is [1, 4000] |
| threadPoolSize | `int` | `16` | streaming and batch | Sets the size of thread pool. |
| useExclusiveReceiver | `boolean` | `true` | streaming and batch | Determines if the connector uses epoch (i.e. exclusive) or non-epoch receiver. The connector uses epoch receivers by default. If you decide to use non-epoch receivers please be aware of its limitation of allowing up to 5 concurrent receivers. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas).  |
| maxSilentTime | `java.time.Duration` | `com.microsoft.azure.eventhubs.EventHubClientOptions.SILENT_OFF` | streaming and batch | Sets the maximum silent time (or `SILENT_OFF`) for a receiver. In case `maxSilentTime` is set to a value other than `SILENT_OFF`, we will try to recreate the receiver if there is no activity for the length of this duration. Note that `maxSilentTime` should be at least `EventHubClientOptions.SILENT_MINIMUM` = 30 seconds.  |
| dynamicPartitionDiscovery | `boolean` | `false` | streaming query | Determines if the number of partitions in the event hubs instance may be updated during the execution. If set to `true`, the number of partitions would be updated every 300 seconds. |
| slowPartitionAdjustment | `boolean` | `false` | streaming query | Determines if the number of events to be read from each partition should be adjusted based on its performance or not. More info is available [here](slow-partition-adjustment-feature.md). |
| maxAcceptableBatchReceiveTime | `java.time.Duration` | 30 seconds | streaming query | Sets the max time that is acceptable for a partition to receive events in a single batch. This value is being used to identify slow partitions when `SlowPartitionAdjustment` is enabled. Only partitions that take more than this time to receive their portion of events in batch are considered as potential slow partitions. More info is available [here](slow-partition-adjustment-feature.md#set-max-acceptable-batch-receive-time). |
| throttlingStatusPlugin | `org.apache.spark.eventhubs.utils.ThrottlingStatusPlugin` | None | streaming query | Sets an object of a class extending the `ThrottlingStatusPlugin` trait to monitor the performance of partitions when `SlowPartitionAdjustment` is enabled. More info is available [here](slow-partition-adjustment-feature.md#monitor-partitions-performances-and-slow-partition-adjustment).  |
| aadAuthCallback | `org.apache.spark.eventhubs.utils.AadAuthenticationCallback` | None | streaming and batch | Sets a callback class extending the `AadAuthenticationCallback` trait to use AAD authentication instead of the connection string to access Event Hubs. More info is available [here](use-aad-authentication-to-connect-eventhubs.md). |
| aadAuthCallbackParams | `Map[String, Object]` | `Map.empty` | streaming and batch | Sets the parameters passed to the AAD authentication callback class. More info is available [here](use-aad-authentication-to-connect-eventhubs.md). |
| metricPlugin | `org.apache.spark.eventhubs.utils.MetricPlugin` | None | streaming and batch | Sets an object of a class extending the `MetricPlugin` trait to monitor send and receive operations performance. `org.apache.spark.eventhubs.utils.SimpleLogMetricPlugin` implements a simple example that just logs the operation performance. |

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
  new NameAndPartition(name, 0) -> EventPosition.fromStartOfStream,
  new NameAndPartition(name, 1) -> EventPosition.fromSequenceNumber(100L)
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
  
val eventhubs = df.select($"body" cast "string")

// Source with per partition starting positions and rate limiting. In this case, we'll start from 
// a sequence number for partition 0, enqueued time for partition 3, the end of stream
// for partition 5, and the start of stream for all other partitions.
val connectionString = "Valid EventHubs connection string."
val name = connectionString.getEventHubName

val positions = Map(
  new NameAndPartition(name, 0) -> EventPosition.fromSequenceNumber(1000L, isInclusive = true),
  new NameAndPartition(name, 3) -> EventPosition.fromEnqueuedTime(Instant.now),
  new NameAndPartition(name, 5) -> EventPosition.fromEndOfStream
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
df.select($"body" cast "string")

// start from same place across all partitions. end at the same place across all partitions.
val ehConf = EventHubsConf("VALID.CONNECTION.STRING")
  .setStartingPosition(EventPosition.fromSequenceNumber(1000L)
  .setEndingPosition(EventPosition.fromEnqueuedTime(Instant.now)

// per partition config
val start = Map(
  new NameAndPartition(name, 0) -> EventPosition.fromSequenceNumber(1000L),
  new NameAndPartition(name, 1) -> EventPosition.fromOffset("100")
)

val end = Map(
  new NameAndPartition(name, 0) -> EventPosition.fromEnqueuedTime(Instant.now),
  new NameAndPartition(name, 1) -> EventPosition.fromSequenceNumber(1000L)
)

val ehConf = EventHubsConf("VALID.CONNECTION.STRING")
  .setStartingPositions(start)
  .setEndingPositions(end)
```


Each row in the source has the following schema:

| Column | Type |
| ------ | ---- |
| body | binary |
| partition | string |
| offset | string |
| sequenceNumber | long |
| enqueuedTime | timestamp |
| publisher | string |
| partitionKey | string |
| properties | map[string, json] |
| systemProperties | map[string, json] |

## Writing Data to EventHubs

Here, we describe the support for writing Streaming Queries and Batch Queries to Azure EventHubs. Take note that, today, 
Azure EventHubs only supports at least once semantics. Consequently, when writing - either Streaming Queries or Batch 
Queries - to  EventHubs, some records may be duplicated; this can happen, for example, if EventHubs needs to retry an 
event that was not acknowledged by the EventHubs service, event if the service received and stored the event. Structured 
Streaming cannot prevent such duplicates from ocurring due to these EventHubs write semantics. However, if writing the 
query is successful, then you can assume that the query output was written at least once. A possible solution to remove 
duplicates when reading the written data could be to introduce a primary (unique) key that can be used to perform de-duplication when reading. 

The Dataframe being written to EventHubs should have the following columns in the schema:

| Column | Type | 
| ------ | ---- | 
| body (required) | string or binary |
| partitionId (*optional) | string |
| partitionKey (*optional) | string |
| properties (optional) | map[string, string] |

* Only one (partitionId or partitionKey) can be set at a time. If both are set, your Structured Streaming job will be stopped. 

The body column is the only required option. If a partitionId and partitionKey are not provided, then events will 
distributed to partitions using a round-robin model. Alternatively, if a partitionId is provided, the query output will 
be sent to that specific partition exclusively. Sending to a single partition is *not* a recommended pattern. Finally, 
if a partionKey is provided, each event will be sent with the provided partitionKey. For more information on how a 
partitionKey works, click [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-programming-guide#partition-key).

Users can also provided properties via a `map[string, string]` if they would like to send any additional properties with their events. 

### Creating an EventHubs Sink for Streaming Queries 

```scala
// Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
val ds = df
  .select("body")
  .writeStream
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // EventHubsConf containing the destination EventHub connection string.
  .start()
  
// Write body data from a DataFrame to EventHubs with a partitionKey
val ds = df
  .selectExpr("partitionKey", "body")
  .writeStream
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // EventHubsConf containing the destination EventHub connection string.
  .start()
```

### Writing the output of Batch Queries to EventHubs

```scala
// Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
df.select("body")
  .write
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // EventHubsConf containing the destination EventHub connection string.
  .save()
  
// Write body data   
df.selectExpr("partitionKey", "body")
  .write
  .format("eventhubs")
  .options(ehWriteConf.toMap)    // EventHubsConf containing the destination EventHub connection string.
  .save()
```

### ForeachWriter

An implementation of `ForeachWriter` is offered by the `EventHubsForeachWriter`. For simple round-robin sends, this is 
the fastest way to write your data from Spark to Event Hubs. For any other send pattern, you 
must use the `EventHubsSink`. A sample is shown below:

```scala
val ehConf = EventHubsConf("YOUR_CONNECTION_STRING") 
val writer = EventHubsForeachWriter(ehConf)

val query =
  streamingSelectDF
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(ProcessingTime("25 seconds"))
    .start()
```

## Recovering from Failures with Checkpointing

The connector fully integrates with the Structured Streaming checkpointing mechanism.
You can recover the progress and state of you query on failures by setting a checkpoint
location in your query. This checkpoint location has to be a path in an HDFS compatible
file system, and can be set as an option in the DataStreamWriter when starting a query.

```python
aggDF \
    .writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .format("memory") \
    .start()
```


## Managing Throughput

When you create an Event Hubs namespace, you are prompted to choose how many throughput units you want for your namespace. 
A single **throughput unit** (or TU) entitles you to:

- Up to 1 MB per second of ingress events (events sent into an event hub), but no more than 1000 ingress events or API calls per second.
- Up to 2 MB per second of egress events (events consumed from an event hub).

With that said, your TUs set an upper bound for the throughput in your streaming application, and this upper bound needs to
be set in Spark as well. In Structured Streaming, this is done with the `maxEventsPerTrigger` option.

Let's say you have 1 TU for a single 4-partition Event Hub instance. This means that Spark is able to consume 2 MB per 
second from your Event Hub without being throttled. If `maxEventsPerTrigger` is set such that Spark consumes *less than 2 MB*, 
then consumption will happen within a second. You're free to leave it as such or you can increase your `maxEventsPerTrigger` 
up to 2 MB per second. If `maxEventsPerTrigger` is set such that Spark consumes *greater than 2 MB*, your micro-batch will 
always take more than one second to be created because consuming from Event Hubs will always take at least one second. 
You're free to leave it as is or you can increase your TUs to increase throughput.

## Serialization of Event Data Properties

Users can pass custom key-value properties in their `EventData`. These properties are exposed in the Spark SQL schema. Keys
are exposed as strings, and values are exposed as json-serialized strings. Native types are supported out of the box. Custom
AMQP types need to be handled explicitly by the connector. Below we list the AMQP types we support and how they are handled:

- `Binary` - the underlying byte array is serialized.
- `Decimal128` - the underlying byte array is serialized.
- `Decimal32` - the underlying integer representation is serialized.
- `Decimal64` - the underlying long representation is serialized.
- `Symbol` - the underlying string is serialized.
- `UnsignedByte` - the underlying string is serialized.
- `UnsignedInteger` - the underlying string is serialized.
- `UnsignedLong` - the underlying string is serialized.
- `UnsignedShort` - the underlying string is serialized.

## Deploying

As with any Spark applications, `spark-submit` is used to launch your application. `azure-eventhubs-spark_2.11`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.21 ...

For experimenting on `spark-shell`, you can also use `--packages` to add `azure-eventhubs-spark_2.11` and its dependencies directly,

    ./bin/spark-shell --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.21 ...

See [Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html) for more details about submitting
applications with external dependencies.
