# Spark Structured Streaming Adjustment for Slow Partitions

This document introduces the new feature in the Spark EventHubs connector which allows spark jobs to automatically adjust 
the number of events to be read from each partition in each batch in presence of slow partitions in the underlying EventHub. 
The goal of such adjustment is to prevent delaying an entire batch because of temporary performance issues in one or a few partitions.


## Table of Contents
* [Introduction](#introduction)
  * [An Example of the Slow Partition Problem](#an-example-of-the-slow-partition-problem)
* [A Solution to the Slow Partition Problem](#a-solution-to-the-slow-partition-problem)
* [User Configuration](#user-configuration)
  * [Enable Slow Partition Adjustment](#enable-slow-partition-adjustment)
  * [Set Max Acceptable Batch Receive Time](#set-max-acceptable-batch-receive-time)
  * [Monitor Partitions Performances and Slow Partition Adjustment](#monitor-partitions-performances-and-slow-partition-adjustment)


## Introduction

As it has been discussed in [Spark Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), 
Structured Streaming queries process data streams as a series of small batch jobs. The Spark EventHubs connector uses the same methodology to run 
Structured Streaming queries. In each batch, it specifies and reads a range of events from each underlying partition. 
Assuming all partitions have sufficient events, each batch tries to read almost an equal number of events from each partition.
Each batch execution is complete when all the specified events from all partitions are read and available on executor nodes.

In this model, the batch execution performance is determined by the performance of the slowest underlying partition. 
This means in a rare situation where one or a few partitions are experiencing performance issues, the batch execution 
takes more time despite the fact that majority of partitions have completed their tasks without any delay.
The following example elaborates this issue:


### An Example of the Slow Partition Problem

Consider a scenario in which we are running a Spark Structured Streaming job to read events from an EventHubs instance with 10 partitions. 
Lets assume the `MaxEventsPerTrigger` is set to 10,000 in our job, which means in each batch we are reading 1,000 events from each partition.
Lets also assume in this set-up reading each event (from any partition) takes about 0.2 ms, which results in reading all events from a single partition in about 200 ms.
If we have enough executor nodes to run all task in parallel, a batch is expected to be completed in about 200 ms.

Now consider a situation in which one of a the partitions (e.g. partition 7) is becoming slower than other partitions and  reading 
an event from that partition takes about 2 ms instead of 0.2 ms. In this case, reading 1,000 events from partition 7 would take 2 seconds.
Since a batch performance is bounded by the slowest partition, the batch also takes 2 seconds to complete despite the fact 
that 90% of partitions have completed their tasks in 200 ms.


## A Solution to the Slow Partition Problem

The above mentioned problem occurs because the number of events to be read from each partition is being assigned without considering partitions performances. 
 In order to resolve this issue, we are adding a feature which monitors the performance of all partitions and automatically adjust the number of events 
 assigned to each partition based on their current performance. This new feature is called `SlowPartitionAdjustment` and is disabled by default.
 
When `SlowPartitionAdjustment` is enabled, we monitor the performance of each partition by measuring the time that takes to read all events in a batch 
from that partition. We compare these times and if any of the underlying partitions is experiencing a performance glitch, we assign less number of 
events (relative to its current performance) to be read from that partition in the next batches until that partition reaches its expected performance.

Coming back to our example, by using the slow partition adjustment feature, the next batch would assign ~100 events to partitions 7 and therefore 
all partitions would complete their assigned tasks in 200 ms. As the result, the next batch would take the expected 200 ms. 


## User Configuration

### Enable Slow Partition Adjustment

In order to use the slow partition feature, `SlowPartitionAdjustment` configuration should be set to true. You can set the `SlowPartitionAdjustment`
in the `EventHubsConf` object. Note that the default value for this configuration is false.

```scala
val ehConf = EventHubsConf(connectionString)
  .setSlowPartitionAdjustment(true)
```

### Set Max Acceptable Batch Receive Time

An important factor in slow partition adjustment feature is identifying the partitions which are experiencing performance issues during batch executions. 
In order to do that, we measure the time that each partition takes to complete its task and mark partitions which take more than average + standard deviation 
time as slow partitions. This method is more sensitive for smaller/faster batches and there is higher chance that it marks partitions as slow in such scenarios. 

In order to avoid this, we have introduced another configuration option, `MaxAcceptableBatchReceiveTime`, which indicates the maximum acceptable time for 
a batch execution in the user defined job. We simply consider all partitions to be performing as expected if the batch execution time does not exceed the 
`MaxAcceptableBatchReceiveTime` and do not mark any partition as slow in such scenarios. This configuration allows users to set the sensitivity of the 
slow partition adjustment feature and trigger its logic only when batch execution time is higher than an expected value. The `MaxAcceptableBatchReceiveTime` 
value can be set in the `EventHubsConf` object and its default value is 30 seconds.

```scala
val ehConf = EventHubsConf(connectionString)
  .setSlowPartitionAdjustment(true)
  .setMaxAcceptableBatchReceiveTime(Duration.ofSeconds(20))
```

Note that although you can set the `MaxAcceptableBatchReceiveTime` value without enabling the `SlowPartitionAdjustment`, it is only being used when the 
`SlowPartitionAdjustment` is enabled.

### Monitor Partitions Performances and Slow Partition Adjustment

If you want to monitor when the slow partition adjustment feature marks a partition as slow and how it adjusts the next batch, you can define a class which extends 
the [`ThrottlingStatusPlugin`](https://github.com/Azure/azure-event-hubs-spark/blob/master/core/src/main/scala/org/apache/spark/eventhubs/utils/ThrottlingStatusPlugin.scala) trait and set an object of that calss in the `EventHubsConf` using the `ThrottlingStatusPlugin` configuration option.

The `ThrottlingStatusPlugin` trait has two methods:

- [`onPartitionsPerformanceStatusUpdate`](https://github.com/Azure/azure-event-hubs-spark/blob/90d70928d9c738923afe5d08557e0a61c9c7188d/core/src/main/scala/org/apache/spark/eventhubs/utils/ThrottlingStatusPlugin.scala#L31) which provides the number of events and the execution time for each partition during the last batch execution in addition to 
the performance percentage metric for each partition which indicates if a partition is running slow or not. The performance percentage metric is a value in the range 
of [0, 1] where a lower value indicates a slower partition and 1 indicates a partition that is performing as expected.

- [`onBatchCreation`](https://github.com/Azure/azure-event-hubs-spark/blob/90d70928d9c738923afe5d08557e0a61c9c7188d/core/src/main/scala/org/apache/spark/eventhubs/utils/ThrottlingStatusPlugin.scala#L27) which provides the range of assigned offsets to be read from each partition in the next batch based on their throttling factor (aka performance percentage metric). 
The throttling factor (aka performance percentage metric) is also provided so that users can see how the performance of each partition affects the number of events in the next batch.


```scala
class SimpleThrottlingStatusPlugin extends ThrottlingStatusPlugin with Logging {
  override def onBatchCreation(
      nextBatchLocalId: Long,
      nextBatchOffsetRanges: Array[OffsetRange],
      partitionsThrottleFactor: mutable.Map[NameAndPartition, Double]): Unit = {
    log.info(
      s"New Batch with localId = $nextBatchLocalId has been created with start and end offsets:" +
        s"${nextBatchOffsetRanges} and partitions performances: ${partitionsThrottleFactor}")
  }

  override def onPartitionsPerformanceStatusUpdate(
      latestUpdatedBatchLocalId: Long,
      partitionsBatchSizes: Map[NameAndPartition, Int],
      partitionsBatchReceiveTimeMS: Map[NameAndPartition, Long],
      partitionsPerformancePercentages: Option[Map[NameAndPartition, Double]]): Unit = {
    log.info(
      s"Latest updated batch with localId = $latestUpdatedBatchLocalId received these information:" +
        s"Batch size: ${partitionsBatchSizes}, batch receive times in ms: ${partitionsBatchReceiveTimeMS}, " +
        s"performance percentages: ${partitionsPerformancePercentages}")
  }
}


val ehConf = EventHubsConf(connectionString)
  .setSlowPartitionAdjustment(true)
  .setThrottlingStatusPlugin(new SimpleThrottlingStatusPlugin)
```

Note that although you can set the `ThrottlingStatusPlugin` value without enabling the `SlowPartitionAdjustment`, it is only being used when the 
`SlowPartitionAdjustment` is enabled.