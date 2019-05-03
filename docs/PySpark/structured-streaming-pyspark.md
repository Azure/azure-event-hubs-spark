# Structured Streaming + Event Hubs Integration Guide for PySpark  

## Table of Contents
* [Linking](#linking)
* [User Configurations](#user-configuration)
  * [Connection String](#connection-string)
  * [Event Hubs Configuration](#event-hubs-configuration)
* [Reading Data from Event Hubs](#reading-data-from-event-hubs)
  * [Creating an Event Hubs Source for Streaming Queries](#creating-an-event-hubs-source-for-streaming-queries)
  * [Creating an Event Hubs Source for Batch Queries](#creating-an-event-hubs-source-for-batch-queries)
* [Writing Data to Event Hubs](#writing-data-to-eventhubs)
  * [Creating an Event Hubs Sink for Streaming Queries](#creating-an-eventhubs-sink-for-streaming-queries)
  * [Writing the output of Batch Queries to Event Hubs](#writing-the-output-of-batch-queries-to-event-hubs)
* [Recovering from Failures with Checkpointing](#recovering-from-failures-with-checkpointing)
* [Managing Throughput](#managing-throughput)
* [Serialization of Event Data Properties](#serialization-of-event-data-properties)
* [Deploying](#deploying)

## Linking

Structured streaming integration for Azure Event Hubs is ultimately run on the JVM, so you'll need to import the libraries from the Maven coordinate below:

```
  groupId = com.microsoft.azure
  artifactId = azure-eventhubs-spark_2.11
  version = 2.3.10
```

For Python applications, you need to add this above library and its dependencies when deploying your application.
See the [Deploying](#deploying) subsection below.

## User Configuration

### Connection String

An Event Hubs connection string is required to connect to the Event Hubs service. You can get the connection string 
for your Event Hubs instance from the [Azure Portal](https://portal.azure.com).

Connection strings must contain an `Endpoint`, `EntityPath` (the Event Hub name), `SharedAccessKeyName`, and `SharedAccessKey`:

    Endpoint=sb://{NAMESPACE}.servicebus.windows.net/{EVENT_HUB_NAME};EntityPath={EVENT_HUB_NAME};SharedAccessKeyName={ACCESS_KEY_NAME};SharedAccessKey={ACCESS_KEY}

### Event Hubs Configuration

All configuration relating to Event Hubs happens in your Event Hubs configuration dictionary. The configuration dictionary must contain an Event Hubs connection string:

```python
connectionString = "YOUR.CONNECTION.STRING"

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString
```

Please read the [Connection String](#connection-string) subsection for more information on obtaining a valid
connection string. 

Additionally, the following configurations are optional:

| Option | value | default | query type | meaning |
| ------ | ----- | ------- | ---------- | ------- |
| eventhubs.consumerGroup | `string` | "$Default" | streaming and batch | A consumer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. More info is available [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-features#event-consumers) | 
| eventhubs.startingPositions | `JSON string` | start of stream | streaming and batch | Sets starting positions for specific partitions. If any positions are set in this option, they take priority over any other option. If nothing is configured within this option, then the setting in `startingPosition` is used. If no position has been set in either option, we will start consuming from the beginning of the partition. |
| eventhubs.startingPosition | `JSON string` | start of stream | streaming and batch | The starting position for your Structured Streaming job. Please read `startingPositions` for detail on which order the options are read. |
| eventhubs.endingPositions | `JSON string` | end of stream | batch query | The ending position of a batch query on a per partition basis. This works the same as `startingPositions`. |
| eventhubs.endingPosition | `JSON string` | end of stream | batch query | The ending position of a batch query. This works the same as `startingPosition`.  | 
|eventhubs.receiverTimeout | `datetime.time` | 60 seconds | streaming and batch | The amount of time Event Hub receive calls will be retried before throwing an exception. | 
| eventhubs.operationTimeout | `datetime.time` | 60 seconds | streaming and batch | The amount of time Event Hub API calls will be retried before throwing an exception. |
| maxEventsPerTrigger | `long` | `partitionCount * 1000` | streaming query | Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. | 

Constructing an Event Hubs configuration dictionary is involved, but the examples below should (hopefully) be a thorough explanation. The following examples all assume the same configuration dictionary initialized [here](#event-hubs-configuration):

#### Consumer Group

```python
ehConf['eventhubs.consumerGroup'] = "DESIRED.CONSUMER.GROUP"
``` 

#### Event Position

The position can be an enqueued time, offset, sequence number, the start of the stream, or the end of the stream. These options are selected by setting the corresponding variable in the table below. Only one of `offset`, `seqNo`, and `enqueuedTime` may be in use at any given time.

| Option | valid values when in use | valid value when not in use |
| ------ | ------------------------ | --------------------------- |
| `offset` | string containing an integer offset,<br>"-1" for start of stream,<br>"@latest" for end of stream | `None` |
| `seqNo` | long | -1 |
| `enqueuedTime` | string of format "YYYY-MM-DDTHH:MM:SS.ssssZ" | `None` |
| `isInclusive` | True, False| N/A |


```python
import datetime from datetime as dt

# Start from beginning of stream
startOffset = "-1"

# End at the current time. This datetime formatting creates the correct string format from a python datetime object
endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

endingEventPosition = {
  "offset": None,           #not in use
  "seqNo": -1,              #not in use
  "enqueuedTime": endTime,
  "isInclusive": True
}


# Put the positions into the Event Hub config dictionary
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)
```

This configuration allows for the batch processing of events occurring from the start of the stream to the current time.

#### Per Partition Configuration

For advanced users, we have provided the option to configure starting and ending positions on a per partition
basis. Consider:

```python
ehName = "YOUR.EVENT.HUB.NAME"

# Create event position for partition 0
positionKey1 = {
  "ehName": ehName,
  "partitionId": 0
}

eventPosition1 = {
  "offset": "@latest",    
  "seqNo": -1,            
  "enqueuedTime": None,   
  "isInclusive": True
}

# Create event position for partition 2
positionKey2 = {
  "ehName": ehName,
  "partitionId": 2
}

eventPosition2 = {
  "offset": None,     
  "seqNo": 100L,        
  "enqueuedTime": None,
  "isInclusive": True
}

# Create more positions for other partitions here if desired

# Put the rules into a map. The position key dictionaries must be made into JSON strings to act as the key.
positionMap = {
  json.dumps(positionKey1) : eventPosition1,
  json.dumps(positionKey2) : eventPosition2
}

# Place the map into the main Event Hub config dictionary
ehConf["eventhubs.startingPositions"] = json.dumps(positionMap)
```

In this case, partition 0 starts reading all new events (i.e. from the end of the partition) while partition 2 starts reading from sequence number 100L. Set up for ending positions follows the same set up.

#### Receiver Timeout and Operation Timeout

Users wanting to customize the receiver or operation timeout settings can do so using the following example:

```python
# Using formatting methods from python's time object to create string durations
receiverTimeoutDuration = datetime.time(0,3,20).strftime("PT%HH%MM%SS") #200 seconds
operationTimeoutDuration = datetime.time(0,1,0).strftime("PT%HH%MM%SS") #60 seconds

# Setting the receiver timeout to 200 seconds
ehConf["eventhubs.receiverTimeout"] = receiverTimeoutDuration

# Setting the receiver timeout to 60 seconds
ehConf["eventhubs.operationTimeout"] = operationTimeoutDuration
```

Note that the timeout duration format is different than the `enqueuedTime` format in the Event Position [section](#event-position). The timeout format must follow the ISO-8601 representation for time (more specifically, it must be a format accepted by `java.time.Duration`).

#### IoT Hub

If using IoT Hub, getting your connection string is the only part of the process that is different - all 
other documentation still applies. Follow these instructions to get your EventHubs-compatible connection string: 

1. Go to the [Azure Portal](https://ms.portal.azure.com) and find your IoT Hub instance
2. Click on **Endpoints** under **Messaging**. Then click on **Events**.
3. Find your ```EventHub-compatible name``` and ```EventHub-compatible endpoint```.

```python
# Note: Remove '{' and '}' when you add your endpoint and eventhub compatible name. 
connectionString = "Endpoint={YOUR.EVENTHUB.COMPATIBLE.ENDPOINT};EntityPath={YOUR.EVENTHUB.COMPATIBLE.NAME}"
ehConf['eventhubs.connectionString'] = connectionString
```

## Reading Data from Event Hubs

### Creating an Event Hubs Source for Streaming Queries 

```python
# Source with default settings
connectionString = "YOUR.CONNECTION.STRING"
ehConf = {
  'eventhubs.connectionString' : connectionString
}

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
```

### Creating an Event Hubs Source for Batch Queries
  
```python
# Source with default settings
connectionString = "YOUR.CONNECTION.STRING"
ehConf = {
  'eventhubs.connectionString' : connectionString
}

# Simple batch query
val df = spark \
  .read \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
df = df.withColumn("body", df["body"].cast("string"))
```

Each row in the source has the following schema:

| Column | Type |
| ------ | ---- |
| body | binary |
| offset | string |
| sequenceNumber | long |
| enqueuedTime | timestamp |
| publisher | string |
| partitionKey | string |
| properties | map[string, json] |
| connectionDeviceID | string |
| systemProperties | map[string, json] |

## Writing Data to Event Hubs

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

```python
# Set up the Event Hub config dictionary with default settings
writeConnectionString = "YOUR.EVENTHUB.NAME"
ehWriteConf = {
  'eventhubs.connectionString' : writeConnectionString
}

# Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
ds = df \
  .select("body") \
  .writeStream \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", "///output.txt") \
  .start()

# Write body data from a DataFrame to EventHubs with a partitionKey
ds = df \
  .selectExpr("partitionKey", "body") \
  .writeStream \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", "///output.txt") \
  .start()
```

### Writing the output of Batch Queries to EventHubs

```python
# Set up the Event Hub config dictionary with default settings
writeConnectionString = "YOUR.EVENTHUB.NAME"
ehWriteConf = {
  'eventhubs.connectionString' : writeConnectionString
}

# Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
ds = df \
  .select("body") \
  .write \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", YOUR.OUTPUT.PATH.STRING) \
  .save()

# Write body data from a DataFrame to EventHubs with a partitionKey
ds = df \
  .selectExpr("partitionKey", "body") \
  .writeStream \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", "///output.txt") \
  .save()
```

## Recovering from Failures with Checkpointing

The connector fully integrates with the Structured Streaming checkpointing mechanism.
You can recover the progress and state of you query on failures by setting a checkpoint
location in your query. This checkpoint location has to be a path in an HDFS compatible
file system, and can be set as an option in the DataStreamWriter when starting a query.

```scala
aggDF
  .writeStream
  .outputMode("complete")
  .option("checkpointLocation", "path/to/HDFS/dir")
  .format("memory")
  .start()
```

## Managing Throughput

When you create an Event Hubs namespace, you are prompted to choose how many throughput units you want for your namespace. 
A single **throughput unit** (or TU) entitles you to:

- Up to 1 MB per second of ingress events (events sent into an event hub), but no more than 1000 ingress events or API calls per second.
- Up to 2 MB per second of egress events (events consumed from an event hub).

With that said, your TUs set an upper bound for the throughput in your streaming application, and this upper bound needs to
be set in Spark as well. In Structured Streaming, this is done with the `maxEventsPerTrigger` option.

Let's say you have 1 TU for a single 4-partition Event Hub instance. This means that Spark is able to consume 2 MB per second 
from your Event Hub without being throttled. If `maxEventsPerTrigger` is set such that Spark consumes *less than 2 MB*, then consumption
will happen within a second. You're free to leave it as such or you can increase your `maxEventsPerTrigger` up to 2 MB per second.
If `maxEventsPerTrigger` is set such that Spark consumes *greater than 2 MB*, your micro-batch will always take more than one second
to be created because consuming from Event Hubs will always take at least one second. You're free to leave it as is or you can increase
your TUs to increase throughput.

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

    ./bin/spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.10 ...

For experimenting on `spark-shell`, you can also use `--packages` to add `azure-eventhubs-spark_2.11` and its dependencies directly,

    ./bin/spark-shell --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.10 ...

See [Application Submission Guide](https://spark.apache.org/docs/latest/submitting-applications.html) for more details about submitting
applications with external dependencies.
