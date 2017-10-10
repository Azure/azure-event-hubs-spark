# Contributor's Guide:<br>Azure Event Hubs + Apache Spark Connector 

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Filing Issues](#filing-issues)
- [Pull Requests](#pull-requests)
    - [General guidelines](#general-guidelines)
    - [Testing guidelines](#testing-guidelines)

## Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/). 

## Getting Started

This library is relatively easy to build. To build and test this locally, make sure you've done the following:
- Java 1.8 SDK is installed
- Maven 3.x is installed
- Scala 2.11.8 is installed
- A supported version of Apache Spark is installed (see [Latest Releases](/README.md#latest-releases) for supported versions). 

After that, clone the code, import it into a Maven project in your favorite IDE. If the above tools are installed, everything should be build. If that's not the case for you OR you need additional help, just contact us by opening an issue in this repo! 
 
## Filing Issues

You can find all of the issues that have been filed in the [Issues](https://github.com/Azure/spark-eventhubs/issues) section of the repository.

If you encounter any bugs, would like to request a feature, or have general questions/concerns/comments, feel free to file an issue [here](https://github.com/Azure/spark-eventhubs/issues/new). **Don't hesitate to reach out!**

## Pull Requests

If you would like to make changes to this library, **break up the change into small, logical, testable chunks, and organize your pull requests accordingly**. This makes for a cleaner, less error-prone development process. 

If you'd like to get involved, but don't know what to work on, then just reach out to us by opening an issue! All contributions/efforts are welcome :) 

You can find all of the pull requests that have been opened in the [Pull Request](https://github.com/Azure/azure-event-hubs-java/pulls) section of the repository.

To open your own pull request, click [here](https://github.com/Azure/spark-eventhubs/compare). Please do your best to detail what's being changed and why! The more detail, the easier it is to review and merge into existing code. 

#### General guidelines

If you're new to opening pull requests - or would like some additional guidance - the following list is a good set of best practices! 

- Title of the pull request is clear and informative
- There are a small number of commits that each have an informative message
- A description of the changes the pull request makes is included, and a reference to the bug/issue the pull request fixes is included, if applicable

#### Testing guidelines

In addition to what has been mentioned, it's important to mention tests! If you add code, make sure you add enough to tests to validate your changes. Again, below is a list of best practices when contributing: 

- Pull request includes test coverage for the included changes
- Test code should not contain hard coded values for resource names or similar values
- Test should not use App.config files for settings

##### We've mentioned it a few times, but, just once more, please feel free to contact us :) Thanks for your interest in the connector! 
