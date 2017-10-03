# spark-eventhubs [![Build Status](https://travis-ci.org/hdinsight/spark-eventhubs.svg?branch=master)](https://travis-ci.org/hdinsight/spark-eventhubs)
This is the source code of EventHubsReceiver for Spark Streaming.

[Here](https://github.com/hdinsight/spark-eventhubs/tree/master/examples) are the examples that use this library to process streaming data from Azure Eventhubs.

For latest integration of EventHubs and Spark Streaming, the document can be found [here](docs/direct_stream.md).


## Latest Release: 2.1.4 (For Spark 2.1.x), 2.0.9 (For Spark 2.0.x), 1.6.3 (For Spark 1.6.x)

[Change Log](docs/change_log.md)

## Usage
For help with Spark Cluster setup and running Spark-EventHubs applications in your cluster, checkout our [Getting Started](docs/getting_started.md) page.

### Getting Officially Released Version

We will have the official release in the maven central repo, you can add the following dependency to your project to reference Spark-EventHubs

#### Maven Dependency
    <!-- https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_[2.10 for spark 1.6. 2.11 for spark 2.0.x or spark 2.1.x] -->
    <dependency>
        <groupId>com.microsoft.azure</groupId>
        <artifactId>spark-streaming-eventhubs_[2.10 for spark 1.6. 2.11 for spark 2.0.x or spark 2.1.x]</artifactId>
        <version>[change it to latest version]</version>
    </dependency>

#### SBT Dependency
    // https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.11
    libraryDependencies += "com.microsoft.azure" % "spark-streaming-eventhubs_[2.10 for spark 1.6. 2.11 for spark 2.0.x or spark 2.1.x]" % "[change it to latest version]"

#### Maven Central for other dependency co-ordinates

https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_[2.10 for spark 1.6. 2.11 for spark 2.0.x or spark 2.1.x]/[change it to latest version]

### Getting Staging Version

We will also publish the staging version of Spark-EventHubs in GitHub. To use the staging version of Spark-EventHubs, the first step is to indicate GitHub as the source repo by adding the following entry to pom.xml:

```xml
<repository>
      <id>spark-eventhubs</id>
      <url>https://raw.github.com/hdinsight/spark-eventhubs/maven-repo/</url>
      <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
      </snapshots>
</repository>
```

You can then add the following dependency to your project to take the pre-released version.

#### Maven Dependency
    <!-- https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.11 -->
    <dependency>
        <groupId>com.microsoft.azure</groupId>
        <artifactId>spark-streaming-eventhubs_[2.10 for spark 1.6. 2.11 for spark 2.0.x or spark 2.1.x]</artifactId>
        <version>2.1.0-SNAPSHOT</version>
    </dependency>

#### SBT Dependency
    // https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.11
    libraryDependencies += "com.microsoft.azure" % "spark-streaming-eventhubs_2.11" % "2.1.0-SNAPSHOT"

## Build Prerequisites

In order to build and run the examples, you need to have:

1. Java 1.8 SDK.
2. Maven 3.x
3. Scala 2.11

## Build Command
    mvn clean
    mvn install 
This command builds and installs spark-streaming-eventhubs jar to local maven cache. Subsequently you can build any Spark Streaming application that references this jar.

## [Integrate Structured Streaming and Azure Event Hubs](docs/ss.md)

## [Integrate Spark Streaming and EventHubs with Direct DStream](docs/direct_stream.md)

