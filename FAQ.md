# Azure Event Hubs Connector for Apache Spark FAQ

**Why am I getting a `ReceiverDisconnectedException`?**

In version 2.3.2 and above, the connector uses epoch receivers from the Event Hubs Java client. 
This only allows one receiver to be open per consumer group-partition combo. To be crystal clear,
let's say we have `receiverA` with an epoch of `0` which is open within consumer group `foo` on partition `0`.
Now, if we open a new receiver, `receiverB`, for the same consumer group and partition with an epoch of
`0` (or higher), then `receiverA` will be disconnected and get the `ReceiverDisconnectedException`. 

In order to avoid this issue, please have one consumer group per Spark application being run. In general, you 
should have a unique consumer group for each consuming application being run. 

**What else? If you have suggestions for this FAQ please share them on the 
[gitter chat](https://gitter.im/azure-event-hubs-spark/Lobby) or open an issue!**