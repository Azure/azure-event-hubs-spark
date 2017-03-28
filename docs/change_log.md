## Change Log

### 2.0.4

* Enable the user to use WASB to store progress files [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Optimize the implementation RestfulClient to minimize the sending request number [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Release with scalaj jars [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Upgrade the Azure EventHubs Client to 0.13 [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Disable the user to use WASB as checkpoint when using receiver based stream [35](https://github.com/hdinsight/spark-eventhubs/pull/35)
* Force SparkContext to shutdown when there is any exception thrown from listener (Workaround the issue that Spark swallows the exceptions thrown from listeners) [41](https://github.com/hdinsight/spark-eventhubs/pull/41)
* Fix the ArrayOutOfRange bug when failed to get highest offsets [48](https://github.com/hdinsight/spark-eventhubs/pull/48https://github.com/hdinsight/spark-eventhubs/pull/48)

#### Breaking Changes

* Due to the breaking changes in EventHubsClient, EventData.properties is typed as Map<String, Object> instead of the original Map<String, String>

### 2.0.3

* Fix the flaky test in receiver based stream [21](https://github.com/hdinsight/spark-eventhubs/pull/21)
* Release Direct DStream [25](https://github.com/hdinsight/spark-eventhubs/pull/25)

### 2.0.2 and previous version

* Receiver based connection 