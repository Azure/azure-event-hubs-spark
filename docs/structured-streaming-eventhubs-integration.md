# Structured Streaming + Azure Event Hubs Integration Guide 

Structured Streaming integration for Azure Event Hubs to read data from Event Hubs. 

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
| consumerGroup | `String` | "$Default" | streaming and batch | A consuer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) | 
| startingPositions | `Map[PartitionId, EventPosition]` | start of stream | streaming and batch | Starting positions for specific partitions. If any positions are set in this option, they take priority when starting the Structured Streaming job. If nothing is configured for a specific partition, then the `EventPosition` set in startingPosition is used. If no position set there, we will start consuming from the beginning of the partition. |
| startingPosition | `EventPosition` | start of stream | streaming and batch | The starting position for your Structured Streaming job. If a specific EventPosition is *not* set for a partition using startingPositions, then we use the `EventPosition` set in startingPosition. If nothing is set in either option, we will begin consuming from the beginning of the partition. |
| endingPositions | `Map[PartitionId, EventPosition]` | end of stream | batch query | The ending position of a batch query on a per partition basis. This works the same as `startingPositions`. |
| endingPosition | `EventPosition` | end of stream | batch query | The ending position of a batch query. This workds the same as `startingPosition`.  | 
| failOnDataLoss | `true` or `false` | true | streaming query | Whether to fail the query when it's possible that data is lost (e.g. when sequence numbers are out of range). This may be a false alarm. You can disable it when it doesn't work as you expected. | 
| maxEventsPerTrigger | `long` | none | streaming query | Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. | 
| receiverTimeout | `java.time.Duration` | 60 seconds | streaming and batch | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| operationTimeout | `java.time.Duration` | 60 seconds | streaming and batch | The amount of time Event Hub API calls will be retried before throwing an exception. |

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
  .as[EventData]

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
  .as[EventData]
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

## Deploying 

As with any Spark applications, `spark-submit` is used to launch your application. `azure-eventhubs-spark_2.11`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.0 ...

For experimenting on `spark-shell`, you can also use `--packages` to add `azure-eventhubs-spark_2.11` and its dependencies directly,

    ./bin/spark-shell --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.0 ...

See [Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html) for more details about submitting
applications with external dependencies.