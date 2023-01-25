# Spark Streaming + Azure Schema Registry

## Register a Schema

First, you need to create a schema group with a schema in a schema registry hosted by Azure Event Hubs. Please refer to [Create an Event Hubs schema registry using the Azure portal](https://docs.microsoft.com/en-us/azure/event-hubs/create-schema-registry) for detailed instructions. 

In this example, we use the following schema. Please follow the steps in the link above and create the below schema in your schema group. Please note down the *Schema GUID* to use in the producer/consumer code later.
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

### Azure Role-Based Access Control

In order to be able to access the schema registry programmatically, you need to register an application in Azure Active Directory (Azure AD) and add the security principal of the application to one of the Azure role-based access control (Azure RBAC) roles mentioned in [Azure role-based access control](https://docs.microsoft.com/en-us/azure/event-hubs/schema-registry-overview#azure-role-based-access-control) section in the schema registery overview page. 
Also, you can refer to [Register an app with Azure AD](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app) for instructions on registering an application using the Azure portal.

Please make sure to note down the client ID (application ID), tenant ID, and the secret to use in the code.

## Producer Examples

In order to send data to an eventhub using the schema registry you need to:
	* Use a property object which contains required information to connect to your schema registry.
	* Create records matching the schema and serialize those using `to_avro` function defined in azure-schemaregistry-spark-avro.
	* Send serialized bytes to the Eventhub instance.

Please note that for accessing the schema registry the below information must be provided in the property map:
    * The schema registry endpoint url, which should be set using the key "schema.registry.url"
    * The tenant ID from the registered application, which should be set using the key "schema.registry.tenant.id"
    * The client ID (application ID) from the registered application, which should be set using the key "schema.registry.client.id"
    * The secret from the registered application, which should be set using the key "schema.registry.client.secret"
    

### Producer Example 1: Using `to_avro` with schema GUID

In order to serialize payloads using the schema GUID, you need to create a property object which contains the required information to access your schema registry and pass the schema GUId to the `to_avro` function. 

#### Create a Schema Registry Object
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import java.util._
 
val schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
val schemaRegistryTenantID = "<YOUR_TENANT_ID>"
val schemaRegistryClientID = "<YOUR_CLIENT_ID>"
val schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"

val props: HashMap[String, String] = new HashMap()
  props.put(SCHEMA_REGISTRY_URL, schemaRegistryURL)
  props.put(SCHEMA_REGISTRY_TENANT_ID_KEY, schemaRegistryTenantID)
  props.put(SCHEMA_REGISTRY_CLIENT_ID_KEY, schemaRegistryClientID)
  props.put(SCHEMA_REGISTRY_CLIENT_SECRET_KEY, schemaRegistryClientSecret)
```

#### Create Records Matching the Schema and Serialize Those Using the Schema GUID
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import com.microsoft.azure.schemaregistry.spark.avro.SchemaGUID
import org.apache.spark.sql.functions.{col, udf}
import spark.sqlContext.implicits._

case class Order(id: String, amount: Double, description: String)
val makeOrderRecord = udf((id: String, amount: Double, description: String) => Order(id, amount, description))

val data = Seq(("id1", 11.11, "order1"), ("id2", 22.22, "order2"), ("id3", 33.33, "order3")) 
val df = data.toDF("id", "amount", "description")

val dfRecord = df.
    withColumn("orders", makeOrderRecord(col("id"), col("amount"), col("description"))).
    drop("id").
    drop("amount").
    drop("description")

val schemaGUIDString: String = "<YOUR_SCHEMA_GUID>"
val dfAvro = dfRecord.select(to_avro($"record", SchemaGUID(schemaGUIDString), props) as "body")
```

#### Send Data to Your Eventhub Instance
```scala
import org.apache.spark.eventhubs._

val connectionString = "Endpoint=sb://<YOUR_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<YOUR_SAS_KEY>;EntityPath=<YOUR_EVENTHUB>"
val ehConf = EventHubsConf(connectionString)

dfAvro.
  write.
  format("eventhubs").
  options(ehConf.toMap).
  save()

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
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import java.util._
 
val schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
val schemaRegistryTenantID = "<YOUR_TENANT_ID>"
val schemaRegistryClientID = "<YOUR_CLIENT_ID>"
val schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"
val schemaGroup = "<YOUR_SCHEMA_GROUP>"
val schemaName = "<YOUR_SCHEMA_NAME>"

val props: HashMap[String, String] = new HashMap()
  props.put(SCHEMA_REGISTRY_URL, schemaRegistryURL)
  props.put(SCHEMA_REGISTRY_TENANT_ID_KEY, schemaRegistryTenantID)
  props.put(SCHEMA_REGISTRY_CLIENT_ID_KEY, schemaRegistryClientID)
  props.put(SCHEMA_REGISTRY_CLIENT_SECRET_KEY, schemaRegistryClientSecret)
  props.put(SCHEMA_GROUP_KEY, schemaGroup)
  props.put(SCHEMA_GROUP_KEY, schemaName)
  // optional: in case you want to enable the schema auto registry, you should set the "schema.auto.register.flag" to "true" in the property map
  // props.put(SCHEMA_AUTO_REGISTER_FLAG_KEY, "true")
```

#### Create Records Matching the Schema and Serialize Those Using the Schema Definition
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import org.apache.spark.sql.functions.{col, udf}
import spark.sqlContext.implicits._

case class Order(id: String, amount: Double, description: String)
val makeOrderRecord = udf((id: String, amount: Double, description: String) => Order(id, amount, description))

val data = Seq(("id1", 11.11, "order1"), ("id2", 22.22, "order2"), ("id3", 33.33, "order3")) 
val df = data.toDF("id", "amount", "description")

val dfRecord = df.
    withColumn("orders", makeOrderRecord(col("id"), col("amount"), col("description"))).
    drop("id").
    drop("amount").
    drop("description")

val schemaString = """
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
 	
val dfAvro = dfRecord.select(to_avro($"record", schemaString, props) as "body")
```

Finally, you can send the payloads in the `dfAvro` to your Eventhub instance using the sample code provided in the [Send Data to Your Eventhub Instance](#send-data-to-your-eventhub-instance) subsection under the producer example 1.


## Consumer Examples

We can perform the following steps to pull data from an Eventhub instance and parse it with respect to a schema from the schema registry:
   * Pull data from the Eventhub instance using azure-event-hubs-spark connector.
   * Use a property object which contains required information to connect to your schema registry.
   * Deserialize the data using `from_avro` function defined in azure-schemaregistry-spark-avro.
   
Please refer to the [Producer Examples](#producer-examples) section for the required information in the property map.
    

### Consumer Example 1: Using `from_avro` with Schema GUID

#### Pull Data
```scala
import org.apache.spark.eventhubs._

val connectionString = "Endpoint=sb://<YOUR_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<YOUR_SAS_KEY>;EntityPath=<YOUR_EVENTHUB>"

val ehConf = EventHubsConf(connectionString).
    setStartingPosition(EventPosition.fromStartOfStream)

val df = spark.
    readStream.
    format("eventhubs").
    options(ehConf.toMap).
    load()
```

#### Create a Schema Registry Object

In case you want to set either `schema.exact.match.required` or `failure.mode` options, you should set their corresponding values in the property map. 
For more information about these options please refer to the [schema registry README](../README.md) file.

```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import java.util._
 
val schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
val schemaRegistryTenantID = "<YOUR_TENANT_ID>"
val schemaRegistryClientID = "<YOUR_CLIENT_ID>"
val schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"

val props: HashMap[String, String] = new HashMap()
  props.put(SCHEMA_REGISTRY_URL, schemaRegistryURL)
  props.put(SCHEMA_REGISTRY_TENANT_ID_KEY, schemaRegistryTenantID)
  props.put(SCHEMA_REGISTRY_CLIENT_ID_KEY, schemaRegistryClientID)
  props.put(SCHEMA_REGISTRY_CLIENT_SECRET_KEY, schemaRegistryClientSecret)
  // optional: in case you want to enable the exact schema match option, you should set the "schema.exact.match.required" to "true" in the property map
  // props.put(SCHEMA_EXACT_MATCH_REQUIRED, "true")
```

#### Deserialize Data
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import com.microsoft.azure.schemaregistry.spark.avro.SchemaGUID

val schemaGUIDString = "<YOUR_SCHEMA_GUID>"
val parsed_df = df.select(from_avro($"body", SchemaGUID(schemaGUIDString), props) as "jsondata")

val query = parsed_df.
    select($"jsondata.id", $"jsondata.amount", $"jsondata.description").
    writeStream.
    format("console").
    start()
```

### Consumer Example 2: Using `from_avro` with Schema Definition

Using `from_avro` with schema definition is very similar to using it with the schema GUID. The first two steps of (I) pulling data from the Eventhub instance and (II) creating a property map 
are exactly the same as steps [Pull Data](#pull-data) and [Create a Schema Registry Object](#create-a-schema-registry-object) in the consumer example 1, respectively.
The only difference is when you use `from_avro` to deserialize the data where you should pass the schema definition instead of the schema GUID.

#### Deserialize Data
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._;

val schemaString = """
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

val parsed_df = df.select(from_avro($"body", schemaString, props) as "jsondata")

val query = parsed_df.
    select($"jsondata.id", $"jsondata.amount", $"jsondata.description").
    writeStream.
    format("console").
    start()
```