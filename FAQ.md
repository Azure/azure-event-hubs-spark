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

**Why am I getting a `ReceiverDisconnectedException`?**

In version 2.3.4 and above, the connector uses epoch receivers from the Event Hubs Java client.
This only allows one receiver to be open per consumer group-partition combo. To be crystal clear,
let's say we have `receiverA` with an epoch of `0` which is open within consumer group `foo` on partition `0`.
Now, if we open a new receiver, `receiverB`, for the same consumer group and partition with an epoch of
`0` (or higher), then `receiverA` will be disconnected and get the `ReceiverDisconnectedException`. 

In order to avoid this issue, please have one consumer group per Spark application being run. In general, you 
should have a unique consumer group for each consuming application being run. 

**What else? If you have suggestions for this FAQ please share them on the 
[gitter chat](https://gitter.im/azure-event-hubs-spark/Lobby) or open an issue!**