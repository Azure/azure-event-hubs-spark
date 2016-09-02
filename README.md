# spark-eventhubs
This is the source code of EventHubsReceiver for Spark Streaming. 

[Here](https://github.com/hdinsight/spark-streaming-data-persistence-examples) is an example project that uses EventHubsReceiver to count and persist messages from Azure Eventhubs.

## Project References

### Maven

#### Repository
    <repository>
      <id>spark-eventhubs</id>
      <url>https://raw.github.com/hdinsight/spark-eventhubs/maven-repo/</url>
      <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
      </snapshots>
    </repository>

#### Dependency
    <dependency>
      <groupId>com.microsoft.azure</groupId>
      <artifactId>spark-streaming-eventhubs_2.10</artifactId>
      <version>2.0.0</version>
    </dependency>
    
### SBT

#### Repository
    scalaVersion := "2.10.0"

    resolvers += "spark-eventhubs" at "https://raw.github.com/hdinsight/spark-eventhubs/maven-repo"

#### Dependency
    libraryDependencies ++= Seq(
        "com.microsoft.azure" %% "spark-streaming-eventhubs" % "2.0.0"
    )

### Build Prerequisites

In order to build and run the examples, you need to have:

1. Java 1.8 SDK.
2. Maven 3.x
3. Scala 2.10

### Build Command
    mvn clean install    
This command builds and installs EventHubsReceiver jar to local maven cache. Subsequently you can build any Spark Streaming application that references this jar.

