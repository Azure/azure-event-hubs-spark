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

## Consumer Examples

We can perform the following steps to pull data from an Eventhub instance and parse it with respect to a schema from the schema registry:
   * Pull data from the Eventhub instance using azure-event-hubs-spark connector.
   * Use a property object which contains required information to connect to your schema registry.
   * Deserialize the data using `from_avro` function defined in azure-schemaregistry-spark-avro.

### Consumer Example: Using `from_avro`

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

```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import java.util._
 
val schemaRegistryURL = "http://<YOUR_NAMESPACE>.servicebus.windows.net"
val schemaRegistryTenantID = "<YOUR_TENANT_ID>"
val schemaRegistryClientID = "<YOUR_CLIENT_ID>"
val schemaRegistryClientSecret = "<YOUR_CLIENT_SECRET>"

val configs: HashMap[String, String] = new HashMap()
  configs.put(SCHEMA_REGISTRY_URL, schemaRegistryURL)
  configs.put(SCHEMA_REGISTRY_TENANT_ID_KEY, schemaRegistryTenantID)
  configs.put(SCHEMA_REGISTRY_CLIENT_ID_KEY, schemaRegistryClientID)
  configs.put(SCHEMA_REGISTRY_CLIENT_SECRET_KEY, schemaRegistryClientSecret)
```

#### Deserialize Data
```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._
import com.microsoft.azure.schemaregistry.spark.avro.SchemaGUID

val schemaGUIDString = "<YOUR_SCHEMA_GUID>"
val parsed_df = df.select(from_avro($"body", SchemaGUID(schemaGUIDString), configs) as "jsondata")

val query = parsed_df.
    select($"jsondata.id", $"jsondata.amount", $"jsondata.description").
    writeStream.
    format("console").
    start()
```