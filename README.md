<p align="center">
  <img src="event-hubs_spark.png" alt="Azure Event Hubs + Apache Spark Connector" width="270"/>
</p>

# Azure Event Hubs Connector for Apache Spark

[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/azure-event-hubs-spark)

|Branch|Status|
|------|-------------|
|master|[![Build Status](https://travis-ci.org/Azure/azure-event-hubs-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-event-hubs-spark)|

This is the source code for the Azure Event Hubs Connector for Apache Spark. 

Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and stream them into multiple applications. 
Spark Streaming and Structured Streaming are scalable and fault-tolerant stream processing engines that allow users to process huge amounts of data using 
complex algorithms expressed with high-level functions like ```map```, ```reduce```, ```join```, and ```window```. This data can then be pushed to 
filesystems, databases, or even back to Event Hubs.  

By making Event Hubs and Spark easier to use together, we hope this connector makes building scalable, fault-tolerant applications easier for our users. 

## Latest Releases

#### Spark
|Spark Version|Package Name|Package Version|
|-------------|------------|----------------|
|Spark 2.3|azure-eventhubs-spark_2.11|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11)|
|Spark 2.2|azure-eventhubs-spark_2.11|[![Maven Central](https://img.shields.io/maven-central/v/com.microsoft.azure/azure-eventhubs-spark_2.11/2.2.0.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11)|

#### Databricks
|Databricks Runtime Version|Package Name|Package Version|
|-------------|------------|----------------|
|Databricks Runtime 4.X|azure-eventhubs-spark_2.11|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11)|
|Databricks Runtime 3.5|azure-eventhubs-spark_2.11|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-spark_2.11)|

#### Roadmap

Planned changes can be found on our [wiki](https://github.com/Azure/azure-event-hubs-spark/wiki).

## Usage

### Linking 

For Scala/Java applications using SBT/Maven project definitions, link your application with the artifact below. 
**Note:** See [Latest Releases](#latest-releases) to find the correct artifiact for your version of Apache Spark (or Databricks)!

    groupId = com.microsoft.azure
    artifactId = azure-eventhubs-spark_[2.XX]</artifactId>
    version = 2.3.0

### Documentation

Documentation for our connector can be found [here](docs/). The integration guides there contain all the information you need to use this library. 

**If you're new to Apache Spark and/or Event Hubs, then we highly recommend reading their documentation first.** You can read Azure Event Hubs 
documentation [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-what-is-event-hubs), documentation for Spark Streaming 
[here](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and, last but not least, Structured Streaming 
[here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). 

### Further Assistance 

**If you need additional assistance, please don't hesitate to ask!** General questions and discussion should happen on our 
[gitter chat](https://gitter.im/azure-event-hubs-spark). Please open an issue for bug reports and feature requests! Feedback, feature 
requests, bug reports, etc are all welcomed!

## Contributing 

If you'd like to help contribute (we'd love to have your help!), then go to our [Contributor's Guide](/.github/CONTRIBUTING.md) for more information. 

## Build Prerequisites

In order to use the connector, you need to have:

1. Java 1.8 SDK.
2. Maven 3.x
3. Scala 2.11

More details on building from source and running tests can be found in our [Contributor's Guide](/.github/CONTRIBUTING.md). 

## Build Command
    
	// Builds jar and runs all tests
	mvn clean package
	
	// Builds jar, runs all tests, and installs jar to your local maven cache
	mvn clean install
	