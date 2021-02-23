# Azure Event Hubs Connector for Apache Spark FAQ

If anything is unclear or if you have feedback, comments, or questions of any kind, then please let us know!
We're available on the [gitter chat](https://gitter.im/azure-event-hubs-spark/Lobby). 

**Why am I seeing: `"The expected sequence number xxx is less than the received sequence number yyy."?`**

Your Event Hubs has a retention policy of 1 to 7 days. Once an event has been in your Event Hubs for 
the length of your retention policy, it will be removed by the service -- I say that the event *expires*. 

In Spark, we schedule a range of events that will be included in the next batch. Let's say we schedule to consume 
from sequence number `X` to `Y` on partition `1`. Then, the Spark executor will try to receive from sequence 
number `X`. There's a case where sequence number `X` is present in the Event Hubs **when we schedule** but 
it expires **before we consume it**. In that case, we'll see the above error message! 

Now that you understand the message, what should we do about it? Well, this happens when your stream is really
behind. Ideally, we want the throughput in Spark to be high enough so that we're always near the latest events. 
Having said that, the best way to handle this error message is to increase your throughput in Spark, increase 
your retention policy in Event Hubs, or both. 

It's also worth noting that you may see this more often in testing scenarios due to irregular send patterns. 
If that's the case, simply send fresh events to the Event Hubs and continue testing with those. 

You may also see this error if there are multiple receivers per consumer group-partition combo. For more information on
how to handle this please read the next question and answer.

**Why am I getting a `ReceiverDisconnectedException`?**

In version 2.3.2 and above, the connector uses epoch receivers from the Event Hubs Java client.
This only allows one receiver to be open per consumer group-partition combo. To be crystal clear,
let's say we have `receiverA` with an epoch of `0` which is open within consumer group `foo` on partition `0`.
Now, if we open a new receiver, `receiverB`, for the same consumer group and partition with an epoch of
`0` (or higher), then `receiverA` will be disconnected and get the `ReceiverDisconnectedException`. 

In order to avoid this issue, please have one consumer group per Spark application being run. In general, you 
should have a unique consumer group for each consuming application being run. 

Note that this error could happen if the same structured stream is accessed by multiple queries (writers).  
Spark will read from the input source and process the dataframe separately for each defined sink. 
This results in having multiple readers on the same consumer group-partition combo.
In order to prevent this, you can create a separate reader for each writer using a separate consumer group or
use an intermediate delta table if you are using Databricks.
      

**Why am I getting events from the `EndofStream`, despite using `setStartingPositions`?**

When you start reading events from Event Hubs, the initial starting positions must be determined. 
If checkpoints are present, then the appropriate offset is found and used. If not, the user-provided `EventHubsConf` 
is queried for starting positions. If none have been set, then we start from the latest sequence numbers available 
in each partition.

There are two options to set the starting positions in `EventHubsConf`. 
`setStartingPosition` which sets the starting position for all partitions and `setStartingPositions` which sets
starting positions on a per partition basis. Note that per partition `setStartingPositions` takes precedent 
over `setStartingPosition`. You can read more about it [here](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#per-partition-configuration).
So if you set the starting positions for a subset of partitions, the starting positions for other partitions would be 
set to the latest sequence numbers available in those partitions.


**How can I fix the `"Send operation timed out"` when I send a batch with large number of events?**

As a general guideline, we don't encourage sending large number of events in a single batch. Large transfers are more 
vulnerable to being interrupted which results in retrying the entire operation regardless of its partial completion.

However, if you decide to send a large number of events within a single batch, you need to make sure that 

(I) You have enough Throughput Units to handle the transfer rate. You can use the **Auto-inflate** feature to automatically
increase the number of throughput unites to meet usage needs.

(II) You have set the timeout value in a way that the send operation has enough time to complete the entire transfer. 
The send operation uses the **`receiverTimeout`** value as the amount of time it allows the operation to get completed. 
Since a single batch is being transferred by a single send operation, if the batch contains a large number events you 
have to adjust the `receiverTimeout` to give enough time to the send operation to complete its entire transfer.


**Why am I seeing: `"java.util.concurrent.TimeoutException: Futures timed out after [5 minutes]"?`**

The connector uses blocking calls on Futures in several places and uses a default 5 minutes timeout to ensure the progress
is not blocked indefinitely. Therefore, generally speaking, the Future time-out exception could happen due to different reasons.

However, a known situation that may result in seeing this error is when the connector keeps recreating the cached receivers 
because they move between different executor nodes. Note that the receiver recreation is expected behavior when 
the receiving task for a specific eventhubs partition is moving from one executor to another. 

In this case, increasing the spark locality can help to reduce/avoid recreating receivers. The spark locality can be 
increased by assigning a higher value to the "spark.locality.wait" property (for instance, increase the value to 15s 
instead of the default value 3s).
 
**What else? If you have suggestions for this FAQ please share them on the 
[gitter chat](https://gitter.im/azure-event-hubs-spark/Lobby) or open an issue!**