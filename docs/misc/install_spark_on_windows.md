Install Spark on Windows
===================

This guide will help you install Apache Spark on your local Windows machine.

----------

### Install Java
- Install Java 8 or later from <a href="http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html" target="_blank">Oracle</a>. 
- To verify the installation is complete, open a command prompt, type ```java -version```, and hit enter. If you see the Java version print out, you're all set.

>**Note:**
>- Take a second to make sure ```JAVA_HOME``` is set in your environment variables. The jdk is usually located here: ```C:\Program Files\Java\jdk<your_version_here>```. 
>- Add ```%JAVA_HOME%\bin``` to your ```PATH``` environment variable if it's not there already. 


### Install Scala
- <a href="https://www.scala-lang.org/download/" target="_blank">Download and install Scala. </a> 
- Set ```SCALA_HOME``` in your environment variables. Typically, scala is installed to ```C:\Program Files (x86)\scala\```.
- Add ```%SCALA_HOME%\bin``` to your ```PATH``` environment variable.

### Install Python
- <a href="https://www.python.org/downloads/windows/" target="_blank">Install Python 2.6 or later</a>

### Download SBT
- <a href="http://www.scala-sbt.org/download.html" target="_blank">Download and install SBT.</a> 
- Set ```SBT_HOME``` as an environment variable with the SBT installation's path. Typically it's ```C:\Program Files (x86)\sbt\```.

### Download ```winutils.exe```
- Download ```winutils.exe``` from the <a href="http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe" target="_blank">HortonWorks repo</a> or 
<a href="https://github.com/steveloughran/winutils/tree/master/hadoop-2.6.0/bin" target="_blank">git repo</a>. 
- Create a folder with the following path: ```C:\Program Files\Hadoop\``` and place ```winutils.exe``` in this new folder.
- Set ```HADOOP_HOME = C:\Program Files\Hadoop\``` in your environment variables.

### Download Apache Spark
- <a href="http://spark.apache.org/downloads.html" target="_blank">Download a **pre-built** Spark package</a> 
- Extract your download. We recommend extracting to ```C:\```, and Spark would be located at ```C:\spark-X.X.X-bin-hadoop2.X\``` 
- Set ```SPARK_HOME``` to Spark's file path
- Add ```%SPARK_HOME%\bin``` to your PATH environment variable.

>Note: 
>- The Spark-EventHubs Connector only works with Apache Spark 2.1.x and earlier! 
>- Your ```SPARK_HOME``` path cannot have any spaces in it - spaces cause issues when starting Spark.	
>For instance, ```Program Files``` cannot be apart of ```SPARK_HOME``` due to the space character in the folder name!

### You're All Set!
- Open a command prompt and run ```spark-shell```
- Open http://localhost:4040/ to view the Spark Web UI 
