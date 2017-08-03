Getting Started With Spark-EventHubs Connector
===================

This guide will help you get a Spark-EventHubs application running in Microsoft Azure. 

----------

Contents
-------------
1.  **Create a Spark Cluster with HDInsight**
2.  **Write Your Spark application**
3.  **Copy JAR to the Spark Cluster**
4.  **_ssh_ into the Spark Cluster**
5.  **Submit Your Spark Application**
6.  **References**

Create a Spark Cluster with HDInsight
---
You'll first need a Spark Cluster, and that's easily achieved by using HDInsight.
<a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apache-spark-jupyter-spark-sql" target="_blank">Click here</a> 
for detailed instructions on how to get the Spark Cluster up and running.

>**Note:**
> Currently, the Spark-EventHubs Connector is only compatible with Spark 2.1.x and earlier. When you create a cluster make sure you choose a compatible version! 


Write Your Spark Application
---
Once your Spark cluster is running, you should write a Spark application. For guidance, check out
<a href="../examples" target="_blank">our examples directory</a> 
and the 
<a href="https://spark.apache.org/docs/latest/streaming-programming-guide.html" target="_blank">Spark Streaming Programming Guide</a>. Check out the [readme](../readme.md) for more info on how to get the Spark-EventHubs connector in your project via Maven or SBT. 

>**Note:**
>When writing/testing locally, make sure you're using the same version of Spark that's on your cluster.


Copy JAR to the Spark Cluster
---
Once your Spark application is written, package your application as a JAR. Then copy the JAR to your cluster. A simple way to do this is using _scp_ which is built into Git Bash on Windows. Simply execute the following command from Git Bash:

```
scp <path/to/JAR> <hdinsight-cluster-name>.azurehdinsight.net:<JAR/destintation>
```

_ssh_ into the Spark Cluster
---
Using your favorite ssh client, ssh into ```<hdinsight-cluster-name>-ssh.azurehdinsight.net``` on ```Port 22```. For more guidance on how to ssh into your Spark Cluster on HDInsight, <a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-linux-use-ssh-unix" target="_blank">click here</a>. 

Submit Your Spark Application
---
Once you've connected to your cluster via ssh, it's time to submit your spark job with ```spark-submit```. Please read the guide on 
<a href="https://spark.apache.org/docs/latest/submitting-applications.html" target="_blank">submitting Spark applications</a> for more help on this. 

>**Note:**
>When you use ```spark-submit```, be sure to set ```master``` to ```yarn-cluster``` and make sure to read about ```executor-memory```, ```total-executor-cores```, and ```num-executors```. [Here](http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/) is a particularly useful blog post on tuning your cluster correctly.  

References
----
- <a href="install_spark_on_windows.md" target="_blank">How to Install Apache Spark on Windows</a>
- <a href="https://spark.apache.org/docs/latest/submitting-applications.html" target="_blank">Submitting Spark Applications</a>
- <a href="https://spark.apache.org/docs/latest/streaming-programming-guide.html" target="_blank">Spark Streaming Programming Guide</a>
- <a href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-what-is-event-hubs" target="_blank">EventHubs Overview</a>
- <a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-apache-spark-overview" target="_blank">Apache Spark on HDInsight</a>
- <a href="http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/" target="_blank">How-to: Tune Your Apache Spark Jobs</a>
