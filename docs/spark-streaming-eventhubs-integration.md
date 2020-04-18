# Spark Streaming + Event Hubs Integration Guide
The Spark Streaming integration for Azure Event Hubs provides simple parallelism, 1:1 correspondence between Event Hubs 
partitions and Spark partitions, and access to sequence numbers and metadata.

## Table of Contents
* [Linking](#linking)
* [User Configurations](#user-configuration)
  * [Connection String](#connection-string)
  * [EventHubsConf](#eventhubsconf)
* [Creating a Direct Stream](#creating-a-direct-stream)
* [Creating an RDD](#creating-an-rdd)
* [Obtaining Offests](#obtaining-offests)
* [Storing Offsets](#storing-offsets)
  * [Checkpoints](#checkpoints)
  * [Your own data store](#your-own-data-store)
* [Recovering from Failures with Checkpointing](#recovering-from-failures-with-checkpointing)
* [Managing Throughput](#managing-throughput)
* [Deploying](#deploying)

## Linking
For Scala/Java applications using SBT/Maven project definitions, link your application with the following artifact:

```
  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.11
  version = 2.3.15

or

  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.12
  version = 2.3.15
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
| consumerGroup | `String` | "$Default" | RDD and DStream | A consumer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) | 
| startingPositions | `Map[NameAndPartition, EventPosition]` | end of stream | RDD and DStream | Starting positions for specific partitions. If any positions are set in this option, they take priority when starting the Structured Streaming job. If nothing is configured for a specific partition, then the `EventPosition` set in startingPosition is used. If no position set there, we will start consuming from the end of the partition. |
| startingPosition | `EventPosition` | end of stream | DStream only | The starting position for your Structured Streaming job. If a specific EventPosition is *not* set for a partition using startingPositions, then we use the `EventPosition` set in startingPosition. If nothing is set in either option, we will begin consuming from the end of the partition. |
| maxRatesPerPartition | `Map[NameAndPartition, Int]` | None | DStream only | Rate limits on a per partition basis. Specify the maximum number of events to be processed on a certain partition within a batch interval. If nothing is set here, `maxRatePerPartition` is used. If nothing is set in there, the default value (1000) is used. | 
| maxRatePerPartition | `Int` | 1000 | DStream only | Rate limit on maximum number of events processed per partition per batch interval. | 
| receiverTimeout | `java.time.Duration` | 60 seconds | RDD and DStream | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| operationTimeout | `java.time.Duration` | 60 seconds | RDD and DStream | The amount of time Event Hub API calls will be retried before throwing an exception. |

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

Additionally, `maxRatesPerPartition` is an available option. Similar to positions, pass a `Map[NameAndPartition, Long]`
to your `EventHubsConf` to configure your max rates on a per partition basis. 

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

## Creating a Direct Stream

```scala
import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition, EventHubsUtils }

val connectionString = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  .setMaxRatePerPartition(10000)

val stream = EventHubsUtils.createDirectStream(streamingContext, ehConf)
```

Each item in the stream is an `EventData`. 

For possible configurations, see the [configuration](#user-configuration) section.

## Creating an RDD

If you have a use case that is better suited to batch processing, you can create an RDD for a defined range of offsets. 

```scala
// Import dependencies and create EventHubsConf as in Create Direct Stream above

val offsetRanges = Array(
  OffsetRange(name = "sample", partitionId = 0, fromSeq = 0, untilSeq = 100),
  OffsetRange(name "sample", partitionId = 1, fromSeq = 0, untilSeq = 100)
)

val rdd = EventHubsUtils.createRDD(sparkContext, ehConf, offsetRanges)
```

## Obtaining Offests

```scala
stream.foreachRDD { rdd => 
  val OffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  rdd.foreachPartition { iter => 
    val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    println(s"${o.name} ${o.partition} ${o.fromSeqNo} ${o.untilSeqNo}")
  }
}
```

Note that the typecast to `HasOffsetRanges` will only succeed if it is done in the first method called on the result of
`createDirectStream`, not later down a chain of methods. Be aware that the one-to-one mapping between RDD partition and
Event Hubs partition does not remain after any methods that shuffle or repartition, e.g. `reduceByKey()` or `window()`.

## Storing Offsets
Delivery semantics in the case of failure depend on how and when sequence numbers are stored. Spark output operations \
are [at-least-once](https://spark.apache.org/docs/latest/streaming-programming-guide.html#semantics-of-output-operations).
So if you want the equivalent of exactly-once semantics, you must either store offsets after an idempotent output, or store 
offsets in an atomic transaction alongside output. With this integration, you have 2 options, in order of increasing 
reliability (and code complexity), for how to store offsets. Note: Event Hubs doesn't support idempotent sends. That feature
is currently under developement. 

### Checkpoints 

If you enable Spark [checkpointing](https://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing), 
sequence numbers from Event Hubs will be stored in the checkpoint. This is easy to enable, but there are drawbacks. 
Your output operation must be idempotent, since you will get repeated outputs; transactions are not an option. Furthermore, 
you cannot recover from a checkpoint if your application code has changed. For planned upgrades, you can mitigate this by 
running the new code at the same time as the old code (since outputs need to be idempotent anyway, they should not clash). 
But for unplanned failures that require code changes, you will lose data unless you have another way to identify known 
good starting offsets.

### Your own data store

For data stores that support transactions, saving sequence numbers from Event Hubs in the same transaction as the results 
can keep the two in sync, even in failure situations. If you're careful about detecting repeated or skipped offset ranges, 
rolling back the transaction prevents duplicated or lost messages from affecting results. This gives the equivalent of 
exactly-once semantics. It is also possible to use this tactic even for outputs that result from aggregations, which are 
typically hard to make idempotent.

```scala
// The details depend on your data store, but the general idea looks like this

// begin from the the offsets committed to the database
val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
  new NameAndPartition(resultSet.string("eventhubName"), resultSet.int("partition")) -> EventPosition
    .fromSequenceNumber(resultSet.long("seqNo"))
}.toMap

// Assuming the EventHubs conf is created elsewhere
ehConf.setStartingPositions(fromOffsets)

val stream = EventHubsUtils.createDirectStream(streamingContext, ehConf)

stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

  val results = yourCalculation(rdd)

  // begin your transaction

  // store `nameAndPartition`, `fromSeqNo`, and/or `untilSeqNo` (all are in offsetRanges)
  // assert that offsetRanges were updated correctly

  // end your transaction
}
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
be set in Spark as well. In Spark Streaming, this is done with `maxRatePerPartition` (or `maxRatesPerPartition` for
per partition configuration). 

Let's say you have 1 TU for a single 4-partition Event Hub instance. This means that Spark is able to consume 2 MB per second 
from your Event Hub without being throttled. Your `batchInterval` needs to be set such that `consumptionTime + processingTime
< batchInterval`. With that said, if your `maxRatePerPartition` is set such that 2 MB or less are consumed within an entire batch
(e.g. across all partitions), then you only need to allocate one second (or less) for `consumptionTime` in your `batchInterval`.
If `maxRatePerPartition` is set such that you have 8 MB per batch (e.g. 8 MB total across all partitions), then your `batchInterval`
then your `batchInterval` must be greater than 4 seconds because `consumptionTime` could be up to 4 seconds. 

## Deploying 
As with any Spark applications, `spark-submit` is used to launch your application.

For Scala and Java applications, if you are using SBT or Maven for project management, then package azure-eventhubs-spark_2.11
and its dependencies into the application JAR. Make sure spark-core_2.11 and spark-streaming_2.11 are marked as provided 
dependencies as those are already present in a Spark installation. Then use `spark-submit` to launch your application 
(see [Deploying](https://spark.apache.org/docs/latest/streaming-programming-guide.html#deploying-applications) section 
in the main programming guide).
