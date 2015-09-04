# spark-eventhubs
This is the source code of EventHubsReceiver for Spark Streaming. 

[Here](https://github.com/hdinsight/hdinsight-spark-examples) is an example project that uses EventHubsReceiver to count messages from Azure EventHubs.

### Prerequisites
In order to build and run the examples, you need to have:

1. Java 1.7/1.8 SDK.
2. Maven 3.x
3. Scala 2.10

### Build
    mvn clean install
This command builds and installs EventHubsReceiver jar to local maven cache. Subsequently you can build any Spark Streaming application that references this jar.

