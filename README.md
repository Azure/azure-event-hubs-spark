<p align="center">
  <img src="event-hubs_spark.png" alt="Azure Event Hubs + Apache Spark Connector" width="270"/>
</p>

# Azure EventHubs + Apache Spark Connector 

|Branch|Status|
|------|-------------|
|master|[![Build Status](https://travis-ci.org/Azure/azure-event-hubs-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-event-hubs-spark)|
|spark-2.1|[![Build Status](https://travis-ci.org/Azure/azure-event-hubs-spark.svg?branch=spark-2.1)](https://travis-ci.org/Azure/azure-event-hubs-spark)|
|spark-2.0|[![Build Status](https://travis-ci.org/Azure/azure-event-hubs-spark.svg?branch=spark-2.0)](https://travis-ci.org/Azure/azure-event-hubs-spark)|
|databricks-3.3|[![Build Status](https://travis-ci.org/Azure/azure-event-hubs-spark.svg?branch=databricks-3.3)](https://travis-ci.org/Azure/azure-event-hubs-spark)|

This is the source code for the Azure Event Hubs and Apache Spark Connector. 

Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and stream them into multiple applications. Spark Streaming and Structured Streaming are scalable and fault-tolerant stream processing engines that allow users to process huge amounts of data using complex algorithms expressed with high-level functions like ```map```, ```reduce```, ```join```, and ```window```. This data can then be pushed to filesystems, databases, or even back to Event Hubs.  

By making Event Hubs and Spark easier to use together, we hope this connector makes building scalable, fault-tolerant applications easier for our users. 

## Latest Releases
#### Spark
|Spark Version|Package Name|Package Version|
|-------------|------------|----------------|
|Spark 2.1|azure-eventhubs-spark_2.11|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11)|
|Spark 2.0|spark-streaming-eventhubs_2.11|[![Maven Central](https://img.shields.io/maven-central/v/com.microsoft.azure/spark-streaming-eventhubs_2.11/2.0.9.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/spark-streaming-eventhubs_2.11)|
|Spark 1.6|spark-streaming-eventhubs_2.10|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/spark-streaming-eventhubs_2.10/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/spark-streaming-eventhubs_2.10)

#### Databricks
|Databricks Runtime Version|Package Name|Package Version|
|-------------|------------|----------------|
|Databricks Runtime 3.3|azure-eventhubs-databricks_2.11|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-databricks_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-databricks_2.11)|

## Overview
The best place to start when using this library is to **make sure you're acquainted with Azure Event Hubs and Apache Spark**. You can read Azure Event Hubs documentation [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-what-is-event-hubs), documentation for Spark Streaming [here](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and, last but not least, Structured Streaming [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). 

#### Using the Connector 
Documentation for our connector can be found [here](docs/) which **includes a Getting Started guide**. Additionally, there're examples using this library [here](/examples). 

#### Further Assistance 
**If you need additional assistance, please don't hesitate to ask!** Just open an issue, and one of the repo owners will get back to you ASAP. :) Feedback, feature requests, bug reports, etc are all welcomed!

## Using the library
In general, you should not need to build this library yourself. If you'd like to help contribute (we'd love to have your help :) ), then building the source and running tests is certainly necessary. You can go to our [Contributor's Guide](/.github/CONTRIBUTING.md) for that information and more. 

This library is available for use in Maven projects from the Maven Central Repository, and can be referenced using the following dependency declaration. Be sure to see the [Latest Releases](#latest-releases) to find the package name and package version that works with your version of Apache Spark!

```XML
    <dependency>
        <groupId>com.microsoft.azure</groupId>
        <artifactId>azure-eventhubs-spark_[2.XX]</artifactId>
        <version>[LATEST]</version>
    </dependency>
	
	<!--- The correct artifactId and version can be found
	in the Latest Releases section above -->
```

#### SBT Dependency
    // https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark_2.11
    libraryDependencies += "com.microsoft.azure" % "azure-eventhubs-spark_[2.XX]" % "[LATEST]"

### Getting the Staging Version
We also publish a staging version of the Azure EventHubs + Apache Spark connector in GitHub. To use the staging version, two things needed to be added to your pom.xml. First add a new repository like so:

```XML
	<repository>
		<id>azure-event-hubs-spark</id>
		<url>https://raw.github.com/Azure/azure-event-hubs-spark/maven-repo/</url>
		<snapshots>
			<enabled>true</enabled>
			<updatePolicy>always</updatePolicy>
		</snapshots>
	</repository>
```

Then add the following dependency declaration:

```XML
    <dependency>
        <groupId>com.microsoft.azure</groupId>
        <artifactId>azure-eventhubs-spark_[2.XX]</artifactId>
        <version>2.1.6-SNAPSHOT</version>
    </dependency>
```

#### SBT Dependency
    // https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark_2.11
    libraryDependencies += "com.microsoft.azure" % "azure-eventhubs-spark_2.11" % "2.1.6-SNAPSHOT"

## Build Prerequisites

In order to use the connector, you need to have:

1. Java 1.8 SDK.
2. Maven 3.x
3. Scala 2.11

More details on building from source and running tests can be found in our [Contributor's Guide](/.github/CONTRIBUTING.md). 

## Build Command
    mvn clean
    mvn install 
This command builds and installs the azure-eventhubs-spark jar to your local maven cache. Subsequently, you can build any Spark Streaming application that references this jar.
