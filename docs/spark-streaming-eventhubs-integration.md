# Spark Streaming + Event Hubs Integration Guide
The Spark Streaming integration for Azure Event Hubs provides simple parallelism, 1:1 correspondence between Event Hubs 
partitions and Spark partitions, and access to sequence numbers and metadata.

## Linking
For Scala/Java applications using SBT/Maven project defnitions, link your application with the follow artifact:

```
  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.11
  version = 2.3.0
```

For Python applications, you need to add this above library and its dependencies when deploying yoru applciation.
See the [Deploying](#deploying) subsection below.

## User Configuration

### Connection String
An Event Hubs connection string is required to connect to the Event Hubs service. You can get the connection string 
for your Event Hubs instance from the [Azure Portal](https://portal.azure.com) or by using the `ConnectionStringBuilder` 
in our library. 

#### IoT Hub
If using IoT Hub, you need to get your EventHubs-equivalent connection string. A guide to do that can be found [here](#).

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
| maxRatesPerPartition | `Map[PartitionId, Long]` | None | N/A | Rate limits on a per partition basis. Specify the maximum number of events to be processed on a certain partition within a batch interval. If nothing is set here, `maxRatePerPartition` is used. If nothing is set in there, the default value (1000) is used. | 
| maxRatePerPartition | `long` | 1000 | N/A | Rate limit on maximum number of events processed per partition per batch interval. | 
| receiverTimeout | `java.time.Duration` | 60 seconds | N/A | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| operationTimeout | `java.time.Duration` | 60 seconds | N/A | The amount of time Event Hub API calls will be retried before throwing an exception. |

## Creating a Direct Stream

```scala
import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition, EventHubsUtils }

val connectionString = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.endOfStream)
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
  // Event Hub name, partition, inclusive starting sequence number, exclusive ending sequence number
  OffsetRange("test", 0, 0, 100),
  OffsetRange("test", 0, 0, 100)
)

val rdd = EventHubsUtils.createRDD(sparkContext, ehConf, offsetRanges)
```

## Obtaining Offests

```scala
stream.foreachRDD { rdd => 
  val OffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  rdd.foreachPartition { iter => 
    val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
    println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
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
can keep the two in sync, even in failure situations. If you’re careful about detecting repeated or skipped offset ranges, 
rolling back the transaction prevents duplicated or lost messages from affecting results. This gives the equivalent of 
exactly-once semantics. It is also possible to use this tactic even for outputs that result from aggregations, which are 
typically hard to make idempotent.

```scala
// The details depend on your data store, but the general idea looks like this

// begin from the the offsets committed to the database
val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
  NameAndPartition(resultSet.string("eventhubName"), resultSet.int("partition")) -> resultSet.long("seqNo")
}.toMap

// Assuming the EventHubs conf is created elsewhere
ehConf.setStartingPositions(fromOffsets)

val stream = EventHubsUtils.createDirectStream(streamingContext, ehConf)

stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

  val results = yourCalculation(rdd)

  // begin your transaction

  // update results
  // update offsets where the end of existing offsets matches the beginning of this batch of offsets
  // assert that offsets were updated correctly

  // end your transaction
}
```

## Deploying 
As with any Spark applications, `spark-submit` is used to launch your application.

For Scala and Java applications, if you are using SBT or Maven for project management, then package azure-eventhubs-spark_2.11
and its dependencies into the application JAR. Make sure spark-core_2.11 and spark-streaming_2.11 are marked as provided 
dependencies as those are already present in a Spark installation. Then use `spark-submit` to launch your application 
(see [Deploying](https://spark.apache.org/docs/latest/streaming-programming-guide.html#deploying-applications) section 
in the main programming guide).
