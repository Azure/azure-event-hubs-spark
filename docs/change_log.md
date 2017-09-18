## Change Log

### 2.1.3 (Sept 18th, 2017)

* Fix issue of unnecessary waiting when customer passes in filtering parameters [141](https://github.com/hdinsight/spark-eventhubs/pull/141)

* Change Receiver Id [135](https://github.com/CodingCat/spark-eventhubs/commit/cf3288a0746c0b1fb5a8cf879834249aa07acf8e)

* Eliminate Listing Operations to handle performance issue with blob storage [134](https://github.com/hdinsight/spark-eventhubs/pull/134)

* Optimize thread synchronization and show metrics of reading progress files [124](https://github.com/hdinsight/spark-eventhubs/pull/124)

* fix NPE in structured streaming [123](https://github.com/hdinsight/spark-eventhubs/pull/123)

* fix flaky test in SS [120](https://github.com/hdinsight/spark-eventhubs/pull/120)

* Add IotHub Spark setup instructions [117](https://github.com/hdinsight/spark-eventhubs/pull/117)

* Add Getting Start Instructions [116](https://github.com/hdinsight/spark-eventhubs/pull/116)

* Update Structured Streaming Doc [129](https://github.com/hdinsight/spark-eventhubs/pull/129)

### 2.0.9 (Sept 18th, 2017)

* Change Receiver Id [138](https://github.com/hdinsight/spark-eventhubs/pull/138)

* Eliminate Listing Operations to handle performance issue with blob storage [136](https://github.com/hdinsight/spark-eventhubs/pull/136)

* Optimize thread synchronization and show metrics of reading progress files [125](https://github.com/hdinsight/spark-eventhubs/pull/125)

### 2.1.2 (July 31st, 2017)

* fix the receiver leaking issue [105](https://github.com/hdinsight/spark-eventhubs/pull/105)

* post receiver id to eventhubs when creating it [109](https://github.com/hdinsight/spark-eventhubs/pull/109)
 
* fix incorrect comments about eventhubs.filter.enqueuetime [96](https://github.com/hdinsight/spark-eventhubs/issues/96)

### 2.0.8 (July 31st, 2017)

* fix the receiver leaking issue [105](https://github.com/hdinsight/spark-eventhubs/pull/105)

* post receiver id to eventhubs when creating it [109](https://github.com/hdinsight/spark-eventhubs/pull/109)
 
* fix incorrect comments about eventhubs.filter.enqueuetime [96](https://github.com/hdinsight/spark-eventhubs/issues/96)

### 2.1.1 (June 25th, 2017)

* replace Rest Client with latest SDK call to query partition info [97](https://github.com/hdinsight/spark-eventhubs/pull/97)

### 2.0.7 (June 25th, 2017)

* replace Rest Client with latest SDK call to query partition info [95](https://github.com/hdinsight/spark-eventhubs/pull/95)

### 2.1.0 (May 25th, 2017)

* Structured Streaming Integration with Azure Event Hubs [77](https://github.com/hdinsight/spark-eventhubs/pull/77)
* Support enqueueTime for 2.1.x [84](https://github.com/hdinsight/spark-eventhubs/pull/84)
* Fix leaked no-daemon thread for 2.1.x [77](https://github.com/hdinsight/spark-eventhubs/pull/77)

### 2.0.6 (May 25th, 2017)

* Support enqueueTime for 2.1.x [74](https://github.com/hdinsight/spark-eventhubs/pull/74)
* Fix leaked no-daemon thread for 2.0.x [68](https://github.com/hdinsight/spark-eventhubs/pull/68)

 
### 1.6.3 (April 11th, 2017)

* fix the race condition in receiver based connection[62](https://github.com/hdinsight/spark-eventhubs/pull/62)

### 2.0.5 (April 11th, 2017)

* fix the race condition in receiver based connection[59](https://github.com/hdinsight/spark-eventhubs/pull/59)

### 2.0.4 (March 28th, 2017)

* Enable the user to use WASB to store progress files [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Optimize the implementation RestfulClient to minimize the sending request number [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Release with scalaj jars [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Upgrade the Azure EventHubs Client to 0.13 [52](https://github.com/hdinsight/spark-eventhubs/pull/52)
* Disable the user to use WASB as checkpoint when using receiver based stream [35](https://github.com/hdinsight/spark-eventhubs/pull/35)
* Force SparkContext to shutdown when there is any exception thrown from listener (Workaround the issue that Spark swallows the exceptions thrown from listeners) [41](https://github.com/hdinsight/spark-eventhubs/pull/41)
* Fix the ArrayOutOfRange bug when failed to get highest offsets [48](https://github.com/hdinsight/spark-eventhubs/pull/48https://github.com/hdinsight/spark-eventhubs/pull/48)
* Optimize Rest Client to retry when there is Http Read timeout [52](https://github.com/hdinsight/spark-eventhubs/pull/52)

#### Breaking Changes

* Due to the breaking changes in EventHubsClient, EventData.properties is typed as Map<String, Object> instead of the original Map<String, String>

### 2.0.3 (Jan 27th, 2017)

* Fix the flaky test in receiver based stream [21](https://github.com/hdinsight/spark-eventhubs/pull/21)
* Release Direct DStream [25](https://github.com/hdinsight/spark-eventhubs/pull/25)

### 2.0.2 and previous version

* Receiver based connection 
