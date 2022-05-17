<p align="center">
  <img src="event-hubs_spark.png" alt="Azure Event Hubs + Apache Spark Connector" width="270"/>
</p>

<h1>Azure Event Hubs Connector for Apache Spark</h1> 

<p>
  <a href="https://gitter.im/azure-event-hubs-spark">
    <img src="https://badges.gitter.im/gitterHQ/gitter.png" alt="chat on gitter">
  </a>
  <a href="https://travis-ci.org/Azure/azure-event-hubs-spark">
    <img src="https://travis-ci.org/Azure/azure-event-hubs-spark.svg?branch=master" alt="build status">
  </a>
  <a href="#star-our-repo">
    <img src="https://img.shields.io/github/stars/azure/azure-event-hubs-spark.svg?style=social&label=Stars" alt="star our repo">
  </a>
</p> 

This is the source code of the Azure Event Hubs Connector for Apache Spark. 

Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and stream them into multiple applications. 
Spark Streaming and Structured Streaming are scalable and fault-tolerant stream processing engines that allow users to process huge amounts of data using 
complex algorithms expressed with high-level functions like `map`, `reduce`, `join`, and `window`. This data can then be pushed to 
filesystems, databases, or even back to Event Hubs.  

By making Event Hubs and Spark easier to use together, we hope this connector makes building scalable, fault-tolerant applications easier for our users. 

## Latest Releases

#### Spark
|Spark Version|Package Name|Package Version|
|-------------|------------|----------------|
|Spark 3.0|azure-eventhubs-spark_2.12|[![Maven Central](https://img.shields.io/badge/maven%20central-2.3.22-brightgreen.svg)](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-eventhubs-spark_2.12%7C2.3.22%7Cjar)|
|Spark 2.4|azure-eventhubs-spark_2.11|[![Maven Central](https://img.shields.io/badge/maven%20central-2.3.22-brightgreen.svg)](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-eventhubs-spark_2.11%7C2.3.22%7Cjar)|
|Spark 2.4|azure-eventhubs-spark_2.12|[![Maven Central](https://img.shields.io/badge/maven%20central-2.3.22-brightgreen.svg)](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-eventhubs-spark_2.12%7C2.3.22%7Cjar)|

#### Databricks
|Databricks Runtime Version|Artifact Id|Package Version|
|-------------|------------|----------------|
|Databricks Runtime 8.X|azure-eventhubs-spark_2.12|[![Maven Central](https://img.shields.io/badge/maven%20central-2.3.22-brightgreen.svg)](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-eventhubs-spark_2.12%7C2.3.22%7Cjar)|
|Databricks Runtime 7.X|azure-eventhubs-spark_2.12|[![Maven Central](https://img.shields.io/badge/maven%20central-2.3.22-brightgreen.svg)](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-eventhubs-spark_2.12%7C2.3.22%7Cjar)|
|Databricks Runtime 6.X|azure-eventhubs-spark_2.11|[![Maven Central](https://img.shields.io/badge/maven%20central-2.3.22-brightgreen.svg)](https://search.maven.org/#artifactdetails%7Ccom.microsoft.azure%7Cazure-eventhubs-spark_2.11%7C2.3.22%7Cjar)|

#### Roadmap

There is an open issue for each planned feature/enhancement. 

## FAQ

We maintain an [FAQ](FAQ.md) - reach out to us via [gitter](https://gitter.im/azure-event-hubs-spark/Lobby) 
if you think anything needs to be added or clarified!

## Usage

### Linking 

For Scala/Java applications using SBT/Maven project definitions, link your application with the artifact below. 
**Note:** See [Latest Releases](#latest-releases) to find the correct artifact for your version of Apache Spark (or Databricks)!

    groupId = com.microsoft.azure
    artifactId = azure-eventhubs-spark_2.11
    version = 2.3.22

or

    groupId = com.microsoft.azure
    artifactId = azure-eventhubs-spark_2.12
    version = 2.3.22

### Documentation

Documentation for our connector can be found [here](docs/). The integration guides there contain all the information you need to use this library. 

**If you're new to Apache Spark and/or Event Hubs, then we highly recommend reading their documentation first.** You can read Event Hubs 
documentation [here](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-what-is-event-hubs), documentation for Spark Streaming 
[here](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and, the last but not least, Structured Streaming 
[here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). 

### Further Assistance 

**If you need additional assistance, please don't hesitate to ask!** General questions and discussion should happen on our 
[gitter chat](https://gitter.im/azure-event-hubs-spark). Please open an issue for bug reports and feature requests! Feedback, feature 
requests, bug reports, etc are all welcomed!

## Contributing 

If you'd like to help contribute (we'd love to have your help!), then go to our [Contributor's Guide](/.github/CONTRIBUTING.md) for more information. 

## Build Prerequisites

In order to use the connector, you need to have:

- Java 1.8 SDK installed
- [Maven 3.x](https://maven.apache.org/download.cgi) installed (or [SBT version 1.x](https://www.scala-sbt.org/1.x/docs/index.html))

More details on building from source and running tests can be found in our [Contributor's Guide](/.github/CONTRIBUTING.md). 

## Build Command
    
	// Builds jar and runs all tests
	mvn clean package
	
	// Builds jar, runs all tests, and installs jar to your local maven repository
	mvn clean install
	
