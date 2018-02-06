
# Strucutred Streaming with Azure EventHubs using PySpark and Jupyter Notebooks

Language Used: **Python3**
Jupyter Kernel Used: **PySpark3**

1. Adding the JAR and configurations. 

```pyspark3
%%configure -f
{"conf": {"spark.jars.packages": "com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.0"}}
```

2. Defining variables needed to connect to Azure Event Hubs.

```pyspark3
# Defining Vars

val connectionString = "YOUR.CONNECTION.STRING"
val ehConf = EventHubsConf(connectionString).toMap
```

3. Connecting to Azure Event Hubs

```pyspark3
# Connecting to EventHubs
inputStream = spark.readStream
.format("eventhubs")
.options(ehConf)
.load()
```

4. Defining a schema for the incoming data

```pyspark3
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType() \
        .add("PlantType", StringType()) \
        .add("Temp", DoubleType()) \
        .add("Light", DoubleType()) \
        .add("Humidity", DoubleType()) \
        .add("Room", IntegerType()) \
        .add("PlantPosition", IntegerType()) \
        .add("SensorDateTime", StringType())
```

5. Running a query.

```pyspark3
# Running a simple filter query

lowlight = inputStream
            .select(from_json(col("body"), schema).alias("data"))
            .where("data.Light < 300")
```

6. Writing the data to MemorySink

```pyspark3
# Writing the data to memory for debugging.

memoryoutput = lowlight.writeStream
				.queryName("LowTemperature")
				.outputMode("append")
				.format("memory")
				.start()
```

7. Ad-hoc querying of your in-memory data

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

8. Stopping the job

```pyspark3
# Stopping the job
memoryoutput.stop()
```