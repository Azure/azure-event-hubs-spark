# spark-eventhubs
This is the source code of EventHubsReceiver for Spark Streaming. 

[Here](https://github.com/hdinsight/spark-streaming-data-persistence-examples/tree/1.x) is an example project that uses EventHubsReceiver to count and persist messages from Azure Eventhubs.

## Project References

### Maven Dependency
    <!-- https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.10 -->
    <dependency>
        <groupId>com.microsoft.azure</groupId>
        <artifactId>spark-streaming-eventhubs_2.10</artifactId>
        <version>1.6.1</version>
    </dependency>

### SBT Dependency

    // https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.10
    libraryDependencies += "com.microsoft.azure" % "spark-streaming-eventhubs_2.10" % "1.6.1"

### Maven Central for other dependency co-ordinates

https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.10/1.6.1

### Build Prerequisites

In order to build and run the examples, you need to have:

1. Java 1.8 SDK.
2. Maven 3.x
3. Scala 2.10

### Build Command
    mvn clean
    mvn package
This command builds and installs EventHubsReceiver jar to local maven cache. Subsequently you can build any Spark Streaming application that references this jar.

