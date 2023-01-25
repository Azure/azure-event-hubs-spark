# Spark Streaming + Azure Schema Registry for PySpark

This document provides producer and consumer examples using the schema registry in PySpark. 
Similar to the [schema registry example for scala](../schema-registry-example.md), first you need to create a schema group with a schema 
in a schema registry and register an application in Azure Active Directory (Azure AD). Please refer to [Register a Schema](../schema-registry-example.md#register-a-schema)
and [Azure Role-Based Access Control](../schema-registry-example.md#azure-role-based-access-control) sections for more information.


## Example Schema
We use the following schema in both producer and consumer examples in this document.
```json
{
    "namespace": "Microsoft.Azure.Data.SchemaRegistry.example",
    "type": "record",
    "name": "Order",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "amount",
            "type": "double"
        },
        {
            "name": "description",
            "type": "string"
        }
    ]
}
```

## Producer Examples
In order to send data to an eventhub using the schema registry in PySpark you need to:
    * Create a wrapper `to_avro` function which works properly in PySpark. 
	* Use a property object which contains required information to connect to your schema registry.
	* Create records matching the schema and serialize those using `to_avro` function.
	* Send serialized bytes to the Eventhub instance.

Please note that for accessing the schema registry the below information must be provided in the property map:
    * The schema registry endpoint url, which should be set using the key "schema.registry.url"
    * The tenant ID from the registered application, which should be set using the key "schema.registry.tenant.id"
    * The client ID (application ID) from the registered application, which should be set using the key "schema.registry.client.id"
    * The secret from the registered application, which should be set using the key "schema.registry.client.secret"
    
 
### Wrapper `to_avro` Function for PySpark
First, we need to define a wrapper function in order to access and use the `com.microsoft.azure.schemaregistry.spark.avro.functions.to_avro` 
from JVM properly.

```python
from pyspark.sql.column import Column, _to_java_column

def to_avro(col, schemaObj, propMap):
    jf = getattr(sc._jvm.com.microsoft.azure.schemaregistry.spark.avro.functions, "to_avro")
    return Column(jf(_to_java_column(col), schemaObj, propMap))
```

### Producer Example 1: Using `to_avro` with schema GUID
In order to serialize payloads using the schema GUID, you need to create a property object which contains the 
required information to access your schema registry and pass the schema GUId to the `to_avro` function. 

#### Create a Schema Registry Object
```python
schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
schemaRegistryTenantID = "<YOUR_TENANT_ID>"
schemaRegistryClientID = "<YOUR_CLIENT_ID>"
schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"

properties = sc._jvm.java.util.HashMap()
properties.put("schema.registry.url", schemaRegistryURL)
properties.put("schema.registry.tenant.id", schemaRegistryTenantID)
properties.put("schema.registry.client.id", schemaRegistryClientID)
properties.put("schema.registry.client.secret", schemaRegistryClientSecret)
```

#### Create Records Matching the Schema and Serialize Those Using the Schema GUID
```python
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.sql.functions import udf

data = [("id1", 11.11, "order1"), ("id2", 22.22, "order2"), ("id3", 33.33, "order3")]
columns = ["id", "amount", "description"]
df = spark.sparkContext.parallelize(data).toDF(columns)

df_schema = StructType([
    StructField('id', StringType(), nullable=False),
    StructField('amount', DoubleType(), nullable=False),
    StructField('description', StringType(), nullable=False)
])

def orderGen(rId, rAmount, rDescription):
    return [rId, rAmount, rDescription]

orderGenUDF = udf(lambda x, y, z : orderGen(x, y, z), df_schema)
dfRecords = df.withColumn("orders", orderGenUDF('id', 'amount', 'description')).drop("id").drop("amount").drop("description")

schemaGUID = "<YOUR_SCHEMA_GUID>"
schemaGuidObj = sc._jvm.com.microsoft.azure.schemaregistry.spark.avro.SchemaGUID(schemaGUID)
dfPayload = dfRecords.withColumn("body", to_avro(dfRecords.orders, schemaGuidObj, properties)).drop("orders")
```


#### Send Data to Your Eventhub Instance
```python
connectionString = "Endpoint=sb://<YOUR_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<YOUR_SAS_KEY>;EntityPath=<YOUR_EVENTHUB>"

ehConf = {}
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

ds = dfPayload.select("body").write.format("eventhubs").options(**ehConf).save()
print("Sent!")
```


### Producer Example 2: Using `to_avro` with Schema Definition
In order to serialize payloads using the schema definition, the property object requires two more values in addition to the required information to access your schema registry:
    * The schema group where your schema has been registered, which should be set using the key "schema.group"
    * The schema name of your registered schema, which should be set using the key "schema.name"
 
Both schema group and schema name are needed to retrieve the unique schema GUID. Note that the schema GUID is being added to every payload so that all consumers know exactly which schema has been used to serialize the payload.

If you want to use a new schema which has not been registered in your schema group, you need to enable the schema auto registry option by setting the `schema.auto.register.flag` to `true` in your property object.
The schema auto registry option simply registers a new schema under the schema group and name provided in the properties object if it cannot find the schema in the given schema group. 
Using a new schema with disabled auto registry option results in an exception. Note that the schema auto registry option is off by default.

Once you create the property map with all the required information, you can use the schema definition instead of the schema GUID in the `to_avro` function. 

#### Create a Schema Registry Object, Including Schema Group and Schema Name
```python
schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
schemaRegistryTenantID = "<YOUR_TENANT_ID>"
schemaRegistryClientID = "<YOUR_CLIENT_ID>"
schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"
schemaGroup = "<YOUR_SCHEMA_GROUP>"
schemaName = "<YOUR_SCHEMA_NAME>"

properties = sc._jvm.java.util.HashMap()
properties.put("schema.registry.url", schemaRegistryURL)
properties.put("schema.registry.tenant.id", schemaRegistryTenantID)
properties.put("schema.registry.client.id", schemaRegistryClientID)
properties.put("schema.registry.client.secret", schemaRegistryClientSecret)
properties.put("schema.group", schemaGroup)
properties.put("schema.name", schemaName)
```

#### Create Records Matching the Schema and Serialize Those Using the Schema Definition
```python
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.sql.functions import udf

data = [("id1", 11.11, "order1"), ("id2", 22.22, "order2"), ("id3", 33.33, "order3")]
columns = ["id", "amount", "description"]
df = spark.sparkContext.parallelize(data).toDF(columns)

df_schema = StructType([
    StructField('id', StringType(), nullable=False),
    StructField('amount', DoubleType(), nullable=False),
    StructField('description', StringType(), nullable=False)
])

def orderGen(rId, rAmount, rDescription):
    return [rId, rAmount, rDescription]

orderGenUDF = udf(lambda x, y, z : orderGen(x, y, z), df_schema)
dfRecords = df.withColumn("orders", orderGenUDF('id', 'amount', 'description')).drop("id").drop("amount").drop("description")

schemaString = """
{
    "namespace": "Microsoft.Azure.Data.SchemaRegistry.example",
    "type": "record",
    "name": "Order",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "amount",
            "type": "double"
        },
        {
            "name": "description",
            "type": "string"
        }
    ]
}
""" 
dfPayload = dfRecords.withColumn("body", to_avro(dfRecords.orders, schemaString, properties)).drop("orders")
```

Finally, you can send the payloads in the `dfAvro` to your Eventhub instance using the sample code provided in the [Send Data to Your Eventhub Instance](#send-data-to-your-eventhub-instance) subsection under the producer example 1.


## Consumer Examples
We can perform the following steps to pull data from an Eventhub instance and parse it with respect to a schema from the schema registry:
   * Create a wrapper `from_avro` function which works properly in PySpark. 
   * Pull data from the Eventhub instance using azure-event-hubs-spark connector.
   * Use a property object which contains required information to connect to your schema registry.
   * Deserialize the data using `from_avro` function.
   
Please refer to the [Producer Examples](#producer-examples) section for the required information in the property map.


### Wrapper `to_avro` Function for PySpark
First, we need to define a wrapper function in order to access and use the `com.microsoft.azure.schemaregistry.spark.avro.functions.to_avro` 
from JVM properly.

```python
from pyspark.sql.column import Column, _to_java_column

def from_avro(col, schemaObj, propMap):
    jf = getattr(sc._jvm.com.microsoft.azure.schemaregistry.spark.avro.functions, "from_avro")
    return Column(jf(_to_java_column(col), schemaObj, propMap))
```

### Consumer Example 1: Using `from_avro` with Schema GUID

#### Pull Data
```python
import json

connectionString = "Endpoint=sb://<YOUR_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<YOUR_SAS_KEY>;EntityPath=<YOUR_EVENTHUB>"

ehConf = {}
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Create the positions
startingEventPosition = {
  "offset": "-1",  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
# Put the positions into the Event Hub config dictionary
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

df = spark.read.format("eventhubs").options(**ehConf).load()
```

#### Create a Schema Registry Object
In case you want to set either `schema.exact.match.required` or `failure.mode` options, you should set their corresponding values in the property map. 
For more information about these options please refer to the [schema registry README](../../README.md) file.

```python
schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
schemaRegistryTenantID = "<YOUR_TENANT_ID>"
schemaRegistryClientID = "<YOUR_CLIENT_ID>"
schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"

properties = sc._jvm.java.util.HashMap()
properties.put("schema.registry.url", schemaRegistryURL)
properties.put("schema.registry.tenant.id", schemaRegistryTenantID)
properties.put("schema.registry.client.id", schemaRegistryClientID)
properties.put("schema.registry.client.secret", schemaRegistryClientSecret)
#optional: in case you want to enable the exact schema match option, you should set the "schema.exact.match.required" to "true" in the property map
#properties.put("schema.exact.match.required", "true")
```

#### Deserialize Data
```python
schemaGUID = "<YOUR_SCHEMA_GUID>"
schemaGuidObj = sc._jvm.com.microsoft.azure.schemaregistry.spark.avro.SchemaGUID(schemaGUID)
parsed_df = df.withColumn("jsondata", from_avro(df.body, schemaGuidObj, properties)).select("jsondata")

ds = parsed_df.select("jsondata.id", "jsondata.amount", "jsondata.description").write.format("console").save()
```

### Consumer Example 2: Using `from_avro` with Schema Definition

Using `from_avro` with schema definition is very similar to using it with the schema GUID. The first two steps of (I) pulling data from the Eventhub instance and (II) creating a property map 
are exactly the same as steps [Pull Data](#pull-data) and [Create a Schema Registry Object](#create-a-schema-registry-object) in the consumer example 1, respectively.
The only difference is when you use `from_avro` to deserialize the data where you should pass the schema definition instead of the schema GUID.

#### Deserialize Data
```python
schemaString = """
{
    "namespace": "Microsoft.Azure.Data.SchemaRegistry.example",
    "type": "record",
    "name": "Order",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "amount",
            "type": "double"
        },
        {
            "name": "description",
            "type": "string"
        }
    ]
}
""" 
parsed_df = df.withColumn("jsondata", from_avro(df.body, schemaString, properties)).select("jsondata")

ds = parsed_df.select("jsondata.id", "jsondata.amount", "jsondata.description").write.format("console").save()
```
