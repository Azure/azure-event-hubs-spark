
# Using the Eventhubs-Spark JAR from Jupyter Notebooks #

Language Used: **Python3**
Jupyter Kernel Used: **PySpark3**

1. Adding the JAR and configuring executors, etc. Rule of thumb is to use **2x of the number of partitions of your EventHubs**.

```pyspark3
%%configure -f
{"executorMemory": "1G", "numExecutors":8, "executorCores":1, "conf": {"spark.jars.packages": "com.microsoft.azure:azure-eventhubs-spark_2.11:2.1.6"}}
```

1. Defining variables needed to connect to Azure EventHubs.

```pyspark3
# Defining Vars

eventHubNamespace = "<Your EH Namespace>"
progressDir = "wasbs:///progressdir/"
policyName = "<Your Policy Name>"
policyKey = "<Your Policy Key>"
eventHubName = "<Your Event Hubs Name>"
consumerGroup = "<Your Consumer Group Name>"
```

1. Connecting to Azure Event Hubs

```pyspark3
# Connecting to EventHubs
inputStream = (spark.readStream
.format("eventhubs")
.option("eventhubs.policyname", policyName)
.option("eventhubs.policykey", policyKey)
.option("eventhubs.namespace", eventHubNamespace)
.option("eventhubs.name", eventHubName)
.option("eventhubs.partition.count", "4")
.option("eventhubs.consumergroup", consumerGroup)
.option("eventhubs.progressTrackingDir", progressDir)
.load())
```

1. Defining a schema for the incoming data

```pyspark3
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Defining the schema to bind to

schema = StructType() \
        .add("PlantType", StringType()) \
        .add("Temp", DoubleType()) \
        .add("Light", DoubleType()) \
        .add("Humidity", DoubleType()) \
        .add("Room", IntegerType()) \
        .add("PlantPosition", IntegerType()) \
        .add("SensorDateTime", StringType())
```

1. Running a Query on your data. Note that since EventHubs body data is serialized as binary, we need to first cast it to a String before performing any queries on it.

```pyspark3
# Running a simple filter query

lowlight = (inputStream
            .selectExpr("CAST(body as STRING)")
            .select(from_json(col("body"), schema).alias("data"))
            .where("data.Light < 300"))
```

1. Writing the data to MemorySink

```pyspark3
# Writing the data to memory for debugging. Once the start() method is called, we start collecting data

memoryoutput = (lowlight.writeStream.queryName("LowTemperature").outputMode("append").format("memory").start())
```

1. Ad-hoc querying of your in-memory data

```pyspark3
# Running a SQL query on the data

spark.sql("SELECT data.PlantType, count(data.PlantType) as CountOfPlants FROM LowTemperature GROUP BY data.PlantType").show()
```

    +----------+-------------+
    | PlantType|CountOfPlants|
    +----------+-------------+
    |      Rose|          203|
    |     Wheat|            3|
    | SugarCane|          869|
    |Watermelon|         1001|
    +----------+-------------+

1. Stopping the job

```pyspark3
# Stopping the job
memoryoutput.stop()
```

1. Archiving/Storing Data to Storage for Batch processing. The data will be partitioned by Year, Month and Day in this case.

```pyspark3
# Setting up Year, Month and Day columns in order to archive the data and partition it by Year, Month, Day
# Also, note that we're pulling in 'enqueuedTime' as well, in order to demonstrate that we can query other EventHubs attributes (such as offset and enqueuedTime as well)

archivedata = inputStream.selectExpr("CAST(body as STRING)", "enqueuedTime").select(from_json(col("body"), schema).alias("data"))
archivedata_w_partitions = (archivedata
.select("data.PlantType", "data.Temp", "data.Light", "data.Humidity", "data.Room", "data.PlantPosition", archivedata.data.SensorDateTime.cast("timestamp").alias("SensorDateTime"))
.withColumn("year", year(col("SensorDateTime").cast("timestamp")))
.withColumn("month", month(col("SensorDateTime").cast("timestamp")))
.withColumn("day", dayofmonth(col("SensorDateTime").cast("timestamp")))
.where("year is not null"))
```

1. Writing the data out to Blob Storage in Parquet format. It uses Snappy compression by default.

```pyspark3
# Writing the data to Blob
archivestream = (archivedata_w_partitions.writeStream
.outputMode("append")
.option("checkpointLocation", "wasbs:///outputprogressdir/")
.format("parquet").option("path", "wasbs:///archivedata/")
.partitionBy("year", "month", "day").start())
```

1. Stopping the archival job.

```pyspark3
# Stopping the Archival job
archivestream.stop()
```
