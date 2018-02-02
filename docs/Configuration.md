# EventHubs + Spark Connector Configuration

- [Required Properties](#required-properties)
    - [Namespace or URI](#namespace-or-uri) 
    - [Additional Required Properties](#additional-required-properties)
    - [Starting Point](#starting-point)
- [Optional Properties](#optional-properties)
    - [General](#general)
    - [Spark Streaming](#spark-streaming)
    - [Structured Streaming](#structured-streaming)
- [Per Partition Configuration](#per-partition-configuration)

__________

All properties used by this library are passed through an `EventHubsConf`. Below is an example showing how the `EventHubsConf` is used with Spark Streaming and Structured Streaming. Notice how we call `toMap` for the Structured Streaming case. 

```scala
val ehConf = new EventHubsConf()
  .setNamespace("sample-ns")
  .setName("sample-name")
  .setKeyName("sample-key-name")
  .setKey("sample-key")

// FOR DSTREAMS
val ehDStream = EventHubsUtils.createDirectStream(ssc, ehConf)

// FOR STRUCTURED STREAMING
val inputStream = spark.readStream
  .format("eventhubs")
  .options(ehConf.toMap) // <--- the toMap call!
  .load
```

Where `ssc` is a [`StreamingContext`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.StreamingContext) and `spark` is a [`SparkSession`](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession).

## Required properties 

### Namespace Or URI
Users can pass their EventHubs namespace name or a URI. Passing a URI is necessary when using non-public clouds running EventHubs.

| Property Name | Default   | Usage | Meaning |
| ------------- |-----------| ------| ------- | 
| `namespace`     | (none)    | `setNamespace(String)` | The namespace name for your EventHubs |
| `URI`   | (none) | `setURI(String)` | The URI for your EventHubs |

### Additional required properties 

| Property Name | Default   | Usage | Meaning |
| ------------- |-----------| ------| ------- | 
| `name`          | (none)    | `setName(String)` | The name of your EventHubs |
| `key name`      | (none)	| `setKeyName(String)` | Your shared access policy name |
| `key` 			| (none)	| `setKey(String)` | Your shared access policy key |
| `partition count` | (none)  | `setPartitionCount(String)` | The number of partitions in your EventHubs | <---- this needs to be deleted
| `progress directory` | (none) | `setProgressDirectory(String)` | All EventHubs related checkpointing will be done to this directory. The directory must be HDFS-compatible | <--- this needs to be deleted

##### TODO: partition count and progress directory need to deprecated. update this documentation when they're gone. 

### Starting Point

##### TODO: What if users want to start from a checkpoint? We need to add a startFromCheckpoint boolean OR we need to make sure we don't force them to provide something when a checkpoint is going to be used. 

In addition to these properties, users are required to pass **one** starting point. Currently, there are four options available to users:

| Property Name | Default   | Usage | Meaning |
| ------------- |-----------| ------| ------- | 
| `start offsets` | -1  | `setStartOffsets(Long)`, `setStartOffsets(Range, Long)` (see [Per Partition Configuration](#per-partition-configuration)) | Spark will start consuming events from these offsets. If only some partitions have been set, the unset partitions will start from the beginning of your EventHubs instance (hence the default is -1). If no partitions have been set, then there are no defaults.  |
| `start enqueue times` | -1 | `setStartEnqueueTimes(Long)`, `setStartEnqueueTimes(Range, Long)` (see [Per Partition Configuration](#per-partition-configuration)) | Spark will start consuming from these enqueue times. If only some partitions have been set, the unset partitions will start from the beginning of your EventHubs instance (hence the default is -1). If no partitions have been set, then there are no defaults. |
| `start of stream` | false	| `setStartOfStream(Boolean)` | When true, all partitions will start from the beginning of your EventHubs instance. |
| `end of stream` | false	| `setEndOfStream(Boolean)` | When true, all partitions will start from the end of your EventHubs instance. |

## Optional properties 

### General

These properties apply to both Spark Streaming and Structured Streaming.

| Property Name | Default   | Usage | Meaning |
| ------------- |-----------| ------| ------- | 
| `receiver timeout` | 5 Seconds | `setReceiverTimeout(java.time.Duration)`| The connector will attempt to receive the expected number of events for this duration before timing out. |
| `operation timeout` | 60 Seconds | `setOperationTimeout(java.time.Duration)`| On failure, EventHubs API calls will retry for this duration before timing out. |

### Spark Streaming

These properties are only applicable to Spark Streaming. If using Structured Streaming, these properties will be ignored.

| Property Name | Default   | Usage | Meaning |
| ------------- |-----------| ------| ------- | 
| `max rate per partition` | 10000    | `setMaxRatePerPartition(Int)`, `setMaxRatePerPartition(Range, Int)` (see [Per Partition Configuration](#per-partition-configuration) | Each batch interval, we will consume this many events per partition (unless there are no more events available in your EventHubs) |

### Structured Streaming

These properties are only applicable to Structured Streaming. If using Spark Streaming, these properties will be ignored.

| Property Name | Default   | Usage | Meaning |
| ------------- |-----------| ------| ------- | 
| `user-defined keys` | (none) | `setSqlUserDefinedKeys(String*)` | ...... |
| `contains properties` | false | `setSqlContainsProperites(Boolean)`  | ........|

## Per Partition Configuration

This library allows user to configure the `maxRatePerPartition`, `startOffsets`, and `startEnqueueTimes` on a per partition basis. This can be done by using `setMaxRatePerPartition(Range, Int)`, `setStartOffsets(Range, Long)`, and `setStartEnqueueTimes(Range, Long)`. If you don't set a value for all partitions, then the default values will be used for the partitions that haven't been set. The default values are show [here](#starting-point). 

The usage for each of these is essentially the same, so we'll show two examples on how these are used. 
_______
If you have any feedback or see any issues, either open a PR or let us know by opening an issue. All help is welcome and apprecaited!
