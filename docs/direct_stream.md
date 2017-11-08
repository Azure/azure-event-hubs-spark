## Integrate Spark Streaming and EventHubs with Direct DStream

In the latest version of our code base, we provide a brand-new approach to integrate Spark Streaming and EventHubs. Being different with the previous approach (Receiver-based), we directly fetch events from EventHubs in distributed Spark tasks which execute the user-defined functions instead of starting a dedicated Spark job to start receiver threads. We call this new approach `Direct DStream` as it is inspired by [the community effort on integrating Spark Streaming and Kafka](https://github.com/apache/spark/tree/master/external/kafka-0-10).

### Overview of Direct DStream

The following figure shows the architecture of workflow of Direct-DStream-based Integration of Spark Streaming and EventHubs. ![Image of Workflow](imgs/workflow_directstream.png).

EventHubDirectDStream is a new type of input stream, which is one-one mapping with a EventHubs namespace and the partitions of the RDD generated in EventHubDirectDStream is one-one mapping to the EventHub partition.

For example, if the user has two namespaces of EventHubs, say `"namespace1"` and `"namespace2"`, each of which contains 2 EventHub instances, "eh1" and "eh2". Both of "eh1" and "eh2" contains 32 partitions. In this scenario, the user will create two EventHubDirectDStream instances with the APIs we will introduce later. The RDDs generated within these EventHubDirectStream will contain 64 partitions each of which maps to partition 0 - 31 of "eh1" and "eh2".

We also provide a progress tracking store component in our implementation. Progress Tracking Store tracks the highest offset processed in each EventHub partition. With this component, users can start from the last saved offset even if they update the application code, which is not able to be achieved with the normal Spark checkpoints.

### How We Use It

The new API is simple to use. In the following code snippet, we create an EventHubDirectDStream.

```scala
val inputDirectStream = EventHubsUtils.createDirectStreams(ssc, "namespace1",
      progressDir, Map(eventHubName -> eventhubParameters))
```

`ssc` is a created StreamingContext object; `"namespace1"` is the namespace of the EventHub you would like to handle with this EventHubDirectDStream; `progressDir` specifies the progress file directory this EventHubDirectDStream works with, and a new directory with the same name as your application will be created under the specified path. The last parameter is a Map from `EventHubName` to `Configuration Entries`, where `Configuration Entries` is also a map. We support the following configuration entries:

* "eventhubs.policyname"
* "eventhubs.policykey"
* "eventhubs.namespace"
* "eventhubs.name"
* "eventhubs.partition.count"
* "eventhubs.consumergroup"
* "eventhubs.maxRate"

While the names of the configuration entries are self-explanatory, we would like to highlight `eventhubs.maxRate`. This parameter regulates the maximum number of messages being processed in a single batch for every EventHub partition and it effectively prevents the job being held up due to a large number of messages being fetched at once.

After getting an EventHubDirectDStream instance, you can apply whatever operations which are available to an ordinary DStream instance with it. We have several example applications in our [example code base](https://github.com/hdinsight/spark-streaming-data-persistence-examples)

### Future Directions

We will continuously improve the stability and performance of EventHub integration. Some of the improvements are as follows:

* We will work with EventHub teams to develop the more performant progress tracking approach with the coordination from EventHub server side.
* We will deliver implementation of design patterns like Lambda/Kappa/Liquid, etc, based on Spark Streaming and EventHub
* We will include the integration with Structured Streaming in the next release
