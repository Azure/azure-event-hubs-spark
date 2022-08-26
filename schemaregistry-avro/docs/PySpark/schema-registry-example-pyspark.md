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

## Consumer Examples
We can perform the following steps to pull data from an Eventhub instance and parse it with respect to a schema from the schema registry:
   * Create a wrapper `from_avro` function which works properly in PySpark. 
   * Pull data from the Eventhub instance using azure-event-hubs-spark connector.
   * Use a property object which contains required information to connect to your schema registry.
   * Deserialize the data using `from_avro` function.

### Wrapper `from_avro` Function for PySpark
First, we need to define a wrapper function in order to access and use the `com.microsoft.azure.schemaregistry.spark.avro.functions.from_avro` 
from JVM properly.

```python
from pyspark.sql.column import Column, _to_java_column

def from_avro(col, schemaGuidObj, configs):
    jf = getattr(sc._jvm.com.microsoft.azure.schemaregistry.spark.avro.functions, "from_avro")
    return Column(jf(_to_java_column(col), schemaGuidObj, configs))
```

### Consumer Example: Using `from_avro`

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

```python
schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
schemaRegistryTenantID = "<YOUR_TENANT_ID>"
schemaRegistryClientID = "<YOUR_CLIENT_ID>"
schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"

configs = sc._jvm.java.util.HashMap()
configs.put("schema.registry.url", schemaRegistryURL)
configs.put("schema.registry.tenant.id", schemaRegistryTenantID)
configs.put("schema.registry.client.id", schemaRegistryClientID)
configs.put("schema.registry.client.secret", schemaRegistryClientSecret)
```

#### Deserialize Data
```python
schemaGUID = "<YOUR_SCHEMA_GUID>"
schemaGuidObj = sc._jvm.com.microsoft.azure.schemaregistry.spark.avro.SchemaGUID(schemaGUID)
parsed_df = df.withColumn("jsondata", from_avro(df.body, schemaGuidObj, configs)).select("jsondata")
ds = parsed_df.select("jsondata.id", "jsondata.amount", "jsondata.description").write.format("console").save()
```
