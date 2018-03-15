# Contributor's Guide:<br>Azure Event Hubs + Apache Spark Connector 

## Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct]
(https://opensource.microsoft.com/codeofconduct/). For more information 
see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/). 

## Getting Started

To build and test this locally, make sure you've done the following:
- Java 1.8 SDK is installed
- [Maven 3.x](https://maven.apache.org/download.cgi) is installed (or [SBT version 1.x](https://www.scala-sbt.org/1.x/docs/index.html))
- A supported version of Apache Spark is installed (see [Latest Releases](/README.md#latest-releases) for supported versions). 

After that, cloning the code and running `mvn clean package` should successfully 
run all unit/integration tests and build a JAR. 
 
## Getting the Staging Version

We also publish a staging version of the Azure EventHubs + Apache Spark connector 
in GitHub. To use the staging version, two things need to be added to your pom.xml. 
First add a new repository like so:

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
        <version>2.3.0</version>
    </dependency>
```

### SBT Dependency

    // https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark_2.11
    libraryDependencies += "com.microsoft.azure" %% "azure-eventhubs-spark" %% "2.3.0"
 
## Filing Issues

You can find all of the issues that have been filed in the [Issues](https://github.com/Azure/spark-eventhubs/issues) 
section of the repository.

If you encounter any bugs, would like to request a feature, or have general 
questions/concerns/comments, feel free to file an issue [here](https://github.com/Azure/spark-eventhubs/issues/new).
**Don't hesitate to reach out!**

## Pull Requests

### Required Guidelines

When filing a pull request, the following must be true:

- Tests have been added (if needed) to validate changes
- scalafmt (using the `.scalafmt.conf` in the repo) must be used to style the code 
- `mvn clean test` must run successfully  

### General Guidelines

If you would like to make changes to this library, **break up the change into small, 
logical, testable chunks, and organize your pull requests accordingly**. This makes 
for a cleaner, less error-prone development process. 

If you'd like to get involved, but don't know what to work on, then just reach out to 
us by opening an issue! All contributions/efforts are welcome :) 

If you're new to opening pull requests - or would like some additional guidance - the 
following list is a good set of best practices! 

- Title of the pull request is clear and informative
- There are a small number of commits that each have an informative message
- A description of the changes the pull request makes is included, and a reference to the bug/issue the pull request fixes is included, if applicable

### Testing Guidelines

If you add code, make sure you add tests to validate your changes. Again, below is a 
list of best practices when contributing: 

- Pull request includes test coverage for the included changes
- Test code should not contain hard coded values for resource names or similar values
- Test should not use App.config files for settings
